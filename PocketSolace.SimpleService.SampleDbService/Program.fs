module PocketSolace.SimpleService.SampleDbService.Program

open System
open System.Collections.Generic
open System.Threading.Channels
open System.Threading.Tasks

open Microsoft.Extensions.Logging

open PocketSolace.SimpleService

/// Like defer from Zig or Go.
let inline private deferAsync ([<InlineIfLambda>] f : unit -> ValueTask) =
    { new IAsyncDisposable with
        override _.DisposeAsync() = f () }

type internal Event =
    | Command of Command * ReplyContext option
    | SendHeartbeat
    | Stop

type internal PubMessage =
    | FeedMessage of Feed
    | Reply of Reply * ReplyContext

type internal Processor(logger : ILogger, db : Dictionary<TradeId, Trade>) =
    let mutable active = true

    interface MessageLoop.IProcessor<Event, PubMessage> with
        override _.Active = active
        override _.Process(input) =
            match input with
            | Command (command, replyContext) ->
                match command with
                | GetAllTrades ->
                    match replyContext with
                    | None ->
                        logger.LogError("Ignoring GetAllTrades without reply context")
                        []
                    | Some replyContext ->
                        let allTrades = db.Values |> Seq.toList |> AllTrades
                        [Reply (allTrades, replyContext)]
                | AddOrUpdateTrade trade ->
                    // For the real db we can just do synchronous write and block the message loop
                    // until the write finishes. If the write can be slow then `Inputs` channel
                    // in `MessageLoop.Parameters` shall be big and the clients of the service shall expect delays.
                    //
                    // The alternative of optimistically pretending that data were immediately written to db
                    // can be risky if the service crashes data can be lost.
                    let found, originalTrade = db.TryGetValue(trade.Id)
                    if found && originalTrade = trade
                    then []  // Command has no effect.
                    else
                        db[trade.Id] <- trade
                        [FeedMessage (UpdatedTrade trade)]
            | SendHeartbeat -> [FeedMessage Heartbeat]
            | Stop ->
                logger.LogInformation("Stopping processor")
                active <- false
                []

[<EntryPoint>]
let internal main args =
    let loggerFactory = LoggerFactory.Create(fun l -> l.AddConsole().SetMinimumLevel(LogLevel.Information) |> ignore)
    let logger = loggerFactory.CreateLogger("Program")

    let solaceConfig = { Host = Environment.GetEnvironmentVariable("SOLACE_HOST")
                         UserName = Environment.GetEnvironmentVariable("SOLACE_USER")
                         Password = Environment.GetEnvironmentVariable("SOLACE_PASSWORD")
                         Vpn = Environment.GetEnvironmentVariable("SOLACE_VPN") }

    let db = Dictionary()
    db["01"] <- { Id = "01"; Price = 10m; Volume = 100m }
    db["02"] <- { Id = "02"; Price = 22m; Volume = 50m }
    db["03"] <- { Id = "03"; Price = 18m; Volume = 120m }

    let serviceInstanceId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() |> string
    let topics = Topics().TopicsForServiceImplementation(serviceInstanceId, solaceConfig.UserName)
    let inputs = Channel.CreateBounded<Event>(512)

    let parameters : MessageLoop.Parameters<Event, PubMessage> =
        { LoggerFactory = loggerFactory

          SolaceConfig = solaceConfig
          SolaceClientNamePrefix = "PocketSolace.SimpleService.SampleDbService"

          SolaceSubscriptions = [topics.CommandSubscription]
          MaxDecompressedPayloadLen = 1024 * 1024
          SolaceInConverter = fun (meta, byteMessage) ->
              // Some messages for the service were discarded.
              // Maybe the service is stuck?
              // It's better to restart.
              // Note: This simple db service isn't listening to any other services.
              //       This means that the only message which could have been discarded is a command.
              //       This is not fatal condition for this service and we can ignore it.
              //       On the other hand when discard happens too frequently the service probably
              //       has some problem and it's better to restart it.
              if meta.BrokerDiscardIndication then
                  failwith "Message discarded"
              [ topics.CommandReader >> Option.map Command ]
              |> TopicsOfService.findAndApplyReader byteMessage
              |> List.singleton
          SolaceOutConverter = fun message ->
              match message with
              | FeedMessage feed -> topics.FeedWriter feed
              | Reply (reply, replyContext) -> topics.ReplyWriter reply replyContext
              |> List.singleton

          Inputs = inputs
          Processor = Processor(logger, db)
        }

    // After the main thread stops this task will stop too.
    backgroundTask {
        use _ = deferAsync (fun () -> inputs.Writer.WriteAsync(Stop))
        while true do
            do! inputs.Writer.WriteAsync(SendHeartbeat)
            do! Task.Delay(5000)
    } |> ignore

    Task.WaitAll(MessageLoop.run parameters)
    logger.LogInformation("Terminating normally :-)")

    0
