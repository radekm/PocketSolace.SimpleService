
open System
open System.Text
open System.Threading.Channels
open System.Threading.Tasks

open Microsoft.Extensions.Logging

open PocketSolace.SimpleService.PubSub
open PocketSolace.SimpleService.SolacePubSub

[<EntryPoint>]
let main args =
    let loggerFactory = LoggerFactory.Create(fun l -> l.AddConsole().SetMinimumLevel(LogLevel.Information) |> ignore)
    let solaceClientLogger = loggerFactory.CreateLogger("Solace Client")
    let pocketSolaceLogger = loggerFactory.CreateLogger("Pocket Solace")
    let pubSubLogger = loggerFactory.CreateLogger("PubSub")

    let testTopic = Environment.GetEnvironmentVariable("SOLACE_TEST_TOPIC")
    if String.IsNullOrWhiteSpace testTopic then
        failwith "SOLACE_TEST_TOPIC environment variable must be non-empty"

    let solace = { Host = Environment.GetEnvironmentVariable("SOLACE_HOST")
                   UserName = Environment.GetEnvironmentVariable("SOLACE_USER")
                   Password = Environment.GetEnvironmentVariable("SOLACE_PASSWORD")
                   Vpn = Environment.GetEnvironmentVariable("SOLACE_VPN") }
    let maxPayloadLen = 1024 * 1024

    let subChannel = Channel.CreateBounded<Message<string> * IncomingMetadata>(512)
    let pubChannel = Channel.CreateBounded<Message<string>>(512)

    let config =
        { SolaceClientLogger = solaceClientLogger
          PocketSolaceLogger = pocketSolaceLogger
          PubSubLogger = pubSubLogger

          Solace = solace
          ClientName = "PocketSolace.SimpleService.Sample"

          Subscriptions = [testTopic]
          SubChannel = subChannel
          SubConverter = fun () ->
              new IncomingConverter<_>(maxPayloadLen, fun (m, meta) ->
                  { Topic = m.Topic
                    ReplyTo = m.ReplyTo
                    ContentType = m.ContentType
                    CorrelationId = m.CorrelationId
                    SenderId = m.SenderId
                    Payload = Encoding.UTF8.GetString m.Payload.Span
                  }, meta)

          PubChannel = pubChannel
          PubConverter = fun () ->
              new OutgoingConverter<_>(maxPayloadLen, fun m ->
                  { Topic = m.Topic
                    ReplyTo = m.ReplyTo
                    ContentType = m.ContentType
                    CorrelationId = m.CorrelationId
                    SenderId = m.SenderId
                    Payload = Bytes (Encoding.UTF8.GetBytes m.Payload |> ReadOnlyMemory)
                  })
        }


    let mainTask = backgroundTask {
        use! _pubSub = connect config

        do! pubChannel.Writer.WriteAsync(
            { Topic = testTopic
              ReplyTo = None
              ContentType = None
              CorrelationId = None
              SenderId = None
              Payload = "Hi, we're testing SimpleService" })

        // Disconnect after 10 seconds.
        backgroundTask {
            do! Task.Delay(10_000)
            Console.WriteLine("Disposing PubSub")
            do! _pubSub.DisposeAsync()
        } |> ignore

        while true do
            let! msg, meta = subChannel.Reader.ReadAsync()
            Console.WriteLine("Got {0} -- {1}", meta, msg)
    }

    Task.WaitAll(mainTask)

    0
