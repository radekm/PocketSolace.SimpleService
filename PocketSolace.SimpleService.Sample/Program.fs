
open System
open System.Text
open System.Threading.Channels
open System.Threading.Tasks

open Microsoft.Extensions.Logging
open PocketSolace

open PocketSolace.SimpleService

/// Like defer from Zig or Go.
let inline private deferAsync ([<InlineIfLambda>] f : unit -> ValueTask) =
    { new IAsyncDisposable with
        override _.DisposeAsync() = f () }

type Event =
    | IncomingMessage of IncomingMetadata * Message<string>
    | SendSignal of int
    | Stop

type Processor(destTopic : string) =
    let mutable active = true

    interface MessageLoop.IProcessor<Event, Message<string>> with
        override _.Active = active
        override _.Process(input) =
            match input with
            | IncomingMessage (meta, message) ->
                Console.WriteLine("Received message {0} -- {1}", meta, message)
                []
            | SendSignal i ->
                Console.WriteLine("Sending message {0}", i)
                [ { Topic = destTopic
                    CorrelationId = None
                    SenderId = None
                    Payload = $"Message %d{i}" } ]
            | Stop ->
                Console.WriteLine("Stopping processor")
                active <- false
                []

[<EntryPoint>]
let main args =
    let enableReconnect = false

    let loggerFactory = LoggerFactory.Create(fun l -> l.AddConsole().SetMinimumLevel(LogLevel.Information) |> ignore)
    let testTopic = Environment.GetEnvironmentVariable("SOLACE_TEST_TOPIC")
    if String.IsNullOrWhiteSpace testTopic then
        failwith "SOLACE_TEST_TOPIC environment variable must be non-empty"

    let solaceConfig = { Host = Environment.GetEnvironmentVariable("SOLACE_HOST")
                         UserName = Environment.GetEnvironmentVariable("SOLACE_USER")
                         Password = Environment.GetEnvironmentVariable("SOLACE_PASSWORD")
                         Vpn = Environment.GetEnvironmentVariable("SOLACE_VPN") }

    let inputs = Channel.CreateBounded<Event>(512)

    let parameters : MessageLoop.Parameters<Event, Message<string>> =
        { LoggerFactory = loggerFactory

          SolaceConfig = solaceConfig
          SolaceClientNamePrefix = "PocketSolace.SimpleService.Sample"

          SolaceSubscriptions = [testTopic]
          MaxDecompressedPayloadLen = 1024 * 1024
          SolaceInConverter = fun (meta, byteMessage) ->
              [ IncomingMessage (meta, byteMessage.MapPayload(fun bytes -> Encoding.UTF8.GetString bytes.Span)) ]
          SolaceOutConverter = fun message ->
              [ message.MapPayload(fun str -> Bytes (Encoding.UTF8.GetBytes str |> ReadOnlyMemory)) ]

          Inputs = inputs
          Processor = Processor(testTopic)
        }

    backgroundTask {
        use _ = deferAsync (fun () -> inputs.Writer.WriteAsync(Stop) )
        for i in 1 .. 100 do
            do! inputs.Writer.WriteAsync(SendSignal i)
            do! Task.Delay(1000)
    } |> ignore

    // Terminates immediately after connection to Solace is lost.
    let messageLoopWithoutReconnect () = MessageLoop.run parameters

    // Note that after disconnect from Solace processor still runs until the disconnect is detected
    // by Solace. This also means that messages send to Solace immediately before disconnect or after disconnect
    // will be lost -- these messages won't be delivered to Solace broker.
    //
    // Additional difficulty comes from background task which is writing `SendSignal i` to the channel `inputs`.
    // This task doesn't pause during disconnect. That means it's still writing to the channel
    // and if the size of the channel wasn't big enough it could fill it completely which
    // in more complex applications could lead to unexpected behavior.
    let messageLoopWithReconnect () = backgroundTask {
        while parameters.Processor.Active do
            Console.WriteLine("Starting message loop")
            try
                do! MessageLoop.run parameters
                if parameters.Processor.Active then
                    // Weird behavior. Should never happen!!!
                    failwith "Bug: Message loop stopped without exception but processor is active"
                else
                    Console.WriteLine("Message loop stopped because processor became inactive")
            with e ->
                Console.WriteLine("Message loop stopped with {0}", e)
    }

    if enableReconnect
    then Task.WaitAll(messageLoopWithReconnect ())
    else Task.WaitAll(messageLoopWithoutReconnect ())
    Console.WriteLine("Terminating normally :-)")

    0
