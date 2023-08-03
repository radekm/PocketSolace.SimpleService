module PocketSolace.SimpleService.SampleDbService.ClientProgram

open System
open System.Collections.Generic
open System.Threading.Channels
open System.Threading.Tasks

open Avalonia
open Avalonia.Controls
open Avalonia.Controls.ApplicationLifetimes
open Avalonia.Layout
open Avalonia.Media
open Avalonia.Themes.Fluent
open Avalonia.Threading

open Microsoft.Extensions.Logging
open PocketSolace.SimpleService

type MainWindow(logger : ILogger) as me =
    inherit Window()

    let currentTrades = Dictionary<TradeId, Trade>()
    let mutable lastUpdatedTrade = None  // Highlight the last updated trade.

    let mainGrid = Grid()
    let status = TextBlock()
    let updaterStack = StackPanel(Orientation = Orientation.Horizontal, Spacing = 10)
    let tradesStack = StackPanel(Orientation = Orientation.Vertical, Spacing = 5)

    do
        me.Padding <- Thickness(10)
        me.Title <- "Db Client"

        mainGrid.RowDefinitions <- RowDefinitions("40,40,*")
        me.Content <- mainGrid

        let statusStack = StackPanel(Orientation = Orientation.Horizontal)
        statusStack.Children.Add <| TextBlock(Text = "Status:")
        statusStack.Children.Add status
        Grid.SetRow(statusStack, 0)
        mainGrid.Children.Add statusStack

        updaterStack.VerticalAlignment <- VerticalAlignment.Center
        updaterStack.Height <- 30
        Grid.SetRow(updaterStack, 1)
        mainGrid.Children.Add updaterStack

        let tradesScroll = ScrollViewer(Content = tradesStack)
        Grid.SetRow(tradesScroll, 2)
        mainGrid.Children.Add tradesScroll

        me.AttachDevTools()

    let updateTrades () =
        tradesStack.Children.Clear()

        currentTrades.Values
        |> Seq.sortByDescending (fun t -> t.Id)
        |> Seq.map (fun t ->
            let stack = StackPanel(Orientation = Orientation.Horizontal)
            if lastUpdatedTrade = Some t.Id then
                stack.Background <- Brushes.GreenYellow
            stack.Children.Add <| TextBlock(Text = $"Id: %s{t.Id}", Width = 100)
            stack.Children.Add <| TextBlock(Text = $"Price: %g{t.Price}", Width = 150)
            stack.Children.Add <| TextBlock(Text = $"Volume: %g{t.Volume}", Width = 150)
            stack :> Control)
        |> tradesStack.Children.AddRange

    // Initially we're disconnected.
    do me.ProcessDisconnect()

    // This implies that client got connected to Solace and got snapshot from db service.
    member me.ProcessTradeSnapshot(trades : Trade list, addOrUpdateTrade : Trade -> unit) =
        status.Text <- "Connected"
        status.Foreground <- Brushes.Green

        // Build UI for trade updater.
        let tradeId = TextBox(Width = 200)
        let price = NumericUpDown(Width = 200)
        let volume = NumericUpDown(Width = 200)
        let submit = Button(Content = TextBlock(Text = "Add or update trade"))
        submit.Click.AddHandler(fun _ _ ->
            if
                tradeId.Text |> String.IsNullOrWhiteSpace |> not &&
                price.Value.HasValue &&
                volume.Value.HasValue
            then
                addOrUpdateTrade { Id = tradeId.Text.Trim(); Price = price.Value.Value; Volume = volume.Value.Value }
                tradeId.Text <- ""
                price.Value <- Nullable()
                volume.Value <- Nullable())
        updaterStack.Children.Add <| TextBlock(Text = "Id")
        updaterStack.Children.Add tradeId
        updaterStack.Children.Add <| TextBlock(Text = "Price")
        updaterStack.Children.Add price
        updaterStack.Children.Add <| TextBlock(Text = "Volume")
        updaterStack.Children.Add volume
        updaterStack.Children.Add submit

        trades |> List.iter (fun t -> currentTrades[t.Id] <- t)
        updateTrades ()

    member me.ProcessTradeUpdate(trade : Trade) =
        currentTrades[trade.Id] <- trade
        lastUpdatedTrade <- Some trade.Id
        updateTrades ()

    member me.ProcessDisconnect() =
        status.Text <- "Disconnected"
        status.Foreground <- Brushes.Red

        updaterStack.Children.Clear()

        currentTrades.Clear()
        lastUpdatedTrade <- None
        updateTrades ()

type internal Event =
    | Tick
    | UIAddOrUpdateTrade of Trade
    | DbServiceReply of Reply * CorrelationId
    | DbServiceFeed of Feed * SenderId

type internal PubMessage =
    | Command of Command * CorrelationId option

type private Processor
    ( logger : ILogger,
      inputChannel : Channel<DateTimeOffset * Event>,
      mainWindow : MainWindow
    ) =

    let mutable feedSenderId = None  // Used to detect restart of db service.
    let mutable feedTime = None  // Used to detect whether db service is alive.

    let snapshotRequestCorrelationId = Guid.NewGuid().ToString()  // We send only one request with this correlation id.
    let mutable snapshotRequestTime = None  // Used to detect that reply with a snapshot hasn't arrive in time.
    let mutable snapshotReceived = false

    let addOrUpdateTrade (trade : Trade) =
        if not (inputChannel.Writer.TryWrite(DateTimeOffset.UtcNow, UIAddOrUpdateTrade trade)) then
            failwith $"Input channel full when adding or updating trade: %A{trade}"

    interface MessageLoop.IProcessor<DateTimeOffset * Event, PubMessage> with
        override _.Active = true
        override _.Process((time, event)) =
            match event with
            | DbServiceFeed (feedMessage, senderId) ->
                match feedSenderId with
                | None -> feedSenderId <- Some senderId
                | Some lastSenderId ->
                    if lastSenderId <> senderId then
                        failwith "Feed sender id changed, service was restarted"
                feedTime <- Some time

                // Pass updates to UI only after a snapshot was passed.
                if snapshotReceived then
                    match feedMessage with
                    | UpdatedTrade trade -> Dispatcher.UIThread.Post(fun () -> mainWindow.ProcessTradeUpdate(trade))
                    | Heartbeat -> ()
                    []
                elif snapshotRequestTime.IsNone then
                    snapshotRequestTime <- Some DateTimeOffset.UtcNow
                    [Command (GetAllTrades, Some snapshotRequestCorrelationId)]
                else
                    []
            | DbServiceReply (reply, correlationId) ->
                if correlationId <> snapshotRequestCorrelationId then
                    logger.LogWarning("Ignoring reply with unknown correlation id")
                    []
                elif snapshotReceived then
                    logger.LogWarning("Ignoring second reply for one correlation id")
                    []
                else
                    snapshotReceived <- true
                    let (AllTrades trades) = reply
                    Dispatcher.UIThread.Post(fun () -> mainWindow.ProcessTradeSnapshot(trades, addOrUpdateTrade))
                    []
            | UIAddOrUpdateTrade trade -> [Command (AddOrUpdateTrade trade, None)]
            | Tick ->
                match feedTime with
                | None -> ()
                | Some lastTime ->
                    // Service should send heartbeat every 5 seconds.
                    if lastTime.AddSeconds 10 < time then
                        failwith "Db service is dead, no heartbeat received"

                match snapshotRequestTime with
                | None -> ()
                | Some snapshotRequestTime ->
                    if not snapshotReceived && snapshotRequestTime.AddSeconds 30 < time then
                        failwith "No reply with all trades received"
                []

type App() =
    inherit Application()

    let loggerFactory = LoggerFactory.Create(fun l -> l.AddConsole().SetMinimumLevel(LogLevel.Information) |> ignore)
    let logger = loggerFactory.CreateLogger("ClientProgram")

    let solaceConfig = { Host = Environment.GetEnvironmentVariable("SOLACE_HOST")
                         UserName = Environment.GetEnvironmentVariable("SOLACE_USER")
                         Password = Environment.GetEnvironmentVariable("SOLACE_PASSWORD")
                         Vpn = Environment.GetEnvironmentVariable("SOLACE_VPN") }

    let clientInstanceId = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() |> string
    let topics = Topics().TopicsForClient("test/trades/db-client", clientInstanceId, solaceConfig.UserName)

    let createParameters inputChannel
        (mainWindow : MainWindow) : MessageLoop.Parameters<DateTimeOffset * Event, PubMessage> =

        { LoggerFactory = loggerFactory

          SolaceConfig = solaceConfig
          SolaceClientNamePrefix = "PocketSolace.SimpleService.SampleDbClient"

          SolaceSubscriptions = [topics.FeedSubscription; topics.ReplySubscription]
          MaxDecompressedPayloadLen = 1024 * 1024
          SolaceInConverter = fun (meta, m) ->
              if meta.BrokerDiscardIndication then
                  failwith "Message discarded"
              let event =
                  [ topics.FeedReader >> Option.map DbServiceFeed
                    topics.ReplyReader >> Option.map DbServiceReply
                  ]
                  |> TopicsOfService.findAndApplyReader m
              [meta.SenderTimestamp, event]
          SolaceOutConverter = fun output ->
              match output with
              | Command (command, correlationId) -> topics.CommandWriter command correlationId
              |> List.singleton

          Inputs = inputChannel
          Processor = Processor(logger, inputChannel, mainWindow)
        }

    let mainWindowTcs = TaskCompletionSource<_>()
    let messageLoopWithReconnect () = backgroundTask {
        let! mainWindow = mainWindowTcs.Task  // Start message loop AFTER main window was initialized.

        while true do
            logger.LogInformation("Starting message loop")
            // Creating new `inputChannel` ensures that old messages are not there.
            let inputChannel = Channel.CreateBounded<DateTimeOffset * Event>(1024)
            let parameters = createParameters inputChannel mainWindow
            try
                let messageLoopTask = MessageLoop.run parameters
                let _tickerTask = backgroundTask {
                    while not messageLoopTask.IsCompleted do
                        do! inputChannel.Writer.WriteAsync((DateTimeOffset.UtcNow, Tick))
                        do! Task.Delay(1000)
                }
                do! messageLoopTask  // Wait for message loop (`_tickerTask` will hopefully terminate too).
                if parameters.Processor.Active then
                    // Weird behavior. Should never happen!!!
                    failwith "Bug: Message loop stopped without exception but processor is active"
                else
                    logger.LogInformation("Message loop stopped because processor became inactive")
            with e ->
                logger.LogError(e, "Message loop stopped with error")

            // Remove contracts in UI.
            Dispatcher.UIThread.Post(fun () -> mainWindow.ProcessDisconnect())

            // Processor became inactive, we exit while loop by raising.
            if not parameters.Processor.Active then
                failwith "Processor became inactive"

            // Wait before trying again.
            do! Task.Delay(10_000)
    }

    let _ = messageLoopWithReconnect ()

    override me.Initialize() =
        me.Styles.Add (FluentTheme())

    override me.OnFrameworkInitializationCompleted() =
        match me.ApplicationLifetime with
        | :? IClassicDesktopStyleApplicationLifetime as desktop ->
            let mainWindow = MainWindow(logger)
            desktop.MainWindow <- mainWindow
            mainWindowTcs.SetResult mainWindow
        | _ -> ()

        base.OnFrameworkInitializationCompleted()

[<CompiledName "BuildAvaloniaApp">]
let buildAvaloniaApp () =
    AppBuilder.Configure<App>()
        .UsePlatformDetect()

[<EntryPoint>]
let main argv = buildAvaloniaApp().StartWithClassicDesktopLifetime(argv)
