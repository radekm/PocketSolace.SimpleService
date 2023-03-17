module PocketSolace.SimpleService.SolacePubSub

open System
open System.Numerics
open System.Threading.Channels
open System.Threading.Tasks

open LibDeflateGzip
open Microsoft.Extensions.Logging
open PocketSolace
open SolaceSystems.Solclient.Messaging

open PocketSolace.SimpleService.PubSub

// -----------------------------------------------------------------------------------
// Config
// -----------------------------------------------------------------------------------

type SolaceConfig =
    { Host : string  // Contains host and port.
      UserName : string
      Password : string
      Vpn : string
    }

// TODO Add support for prefixing all topics.
/// Configuration for constructing `IPubSub` which
/// - subscribes on Solace to topics `Subscriptions`,
/// - converts incoming messages from `IMessage` to `'Incoming` by `SubConverter` and  writes them to `SubChannel`,
/// - reads outgoing messages from `PubChannel` and converts them from `'Outgoing` to `IMessage` by `PubConverter`.
type SolacePubSubConfig<'Incoming, 'Outgoing> =
    { SolaceClientLogger : ILogger
      PocketSolaceLogger : ILogger
      PubSubLogger : ILogger

      Solace : SolaceConfig
      ClientName : string

      Subscriptions : string list
      SubChannel : Channel<'Incoming>
      // `SubConverter` is a factory so it's clear that owner of the struct doesn't have to worry
      // about its disposal.
      SubConverter : unit -> IConverter<IMessage, 'Incoming>

      PubChannel : Channel<'Outgoing>
      // `PubConverter` is a factory so it's clear that owner of the struct doesn't have to worry
      // about its disposal.
      PubConverter : unit -> IConverter<'Outgoing, IMessage>
    }

// -----------------------------------------------------------------------------------
// Converters
// -----------------------------------------------------------------------------------

/// This class promotes function `f : RawMessage * IncomingMetadata -> 'Incoming`
/// to `IConverter<IMessage, 'Incoming>`.
/// The advantage is that types `RawMessage` and `IncomingMetadata`
/// are normal F# records which are easier to work with than `IMessage`.
///
/// Payload length is limited by `maxPayloadLen` to prevent decompression bombs.
/// `f` shall raise if conversion is not possible.
[<Sealed>]
type IncomingConverter<'Incoming>(maxPayloadLen : int, f : RawMessage * IncomingMetadata -> 'Incoming) =
    let mutable disposed = false
    let decompressor = new Decompressor()
    let extraSpace = 128  // Decompressor needs slightly more space.
    let buffer : byte[] = Array.zeroCreate (maxPayloadLen + extraSpace)

    let error msg = failwith $"Cannot convert incoming message: %s{msg}"

    /// Raises exception when message `m` cannot be converted.
    ///
    /// `m` can be converted iff following conditions hold:
    /// - `m.SenderTimestamp` is not negative.
    /// - `m.Destination.Name` is not null.
    /// - `m.HttpContentEncoding` is either null or `gzip`.
    /// - If `m.HttpContentEncoding` is `gzip`
    ///   then `m.BinaryAttachment` contains gzip with exactly one member.
    /// - Size of decompressed `m.BinaryAttachment` is less than or equal `maxPayloadLen`.
    ///
    /// `m` is not disposed.
    let convertToRawMessage (m : IMessage) : RawMessage * IncomingMetadata =
        // We cache `BinaryAttachment` because we believe that every
        // get of `BinaryAttachment` property creates a new copy of attachment.
        let origPayload =
            match m.BinaryAttachment with
            | null -> [||]  // Empty array given to `BinaryAttachment` setter is translated to `null`.
            | attachment -> attachment

        if m.SenderTimestamp < 0 then
            error $"negative %s{nameof m.SenderTimestamp}"
        if isNull m.Destination || isNull m.Destination.Name then
            error $"%s{nameof m.Destination}.%s{nameof m.Destination.Name} not defined"
        if not (isNull m.HttpContentEncoding || m.HttpContentEncoding = "gzip") then
            error $"unsupported %s{nameof m.HttpContentEncoding} %A{m.HttpContentEncoding}"

        let decompressedPayload =
            match m.HttpContentEncoding with
            | null -> ReadOnlyMemory origPayload
            | "gzip" ->
                let result, read, written = decompressor.Decompress(origPayload, buffer)
                match result with
                | DecompressionResult.Success ->
                    if read < origPayload.Length then
                        error $"%s{nameof m.BinaryAttachment} contains more than one gzip member"
                    ReadOnlyMemory buffer[0 .. written - 1]
                | DecompressionResult.BadData ->
                    error $"%s{nameof m.BinaryAttachment} contains invalid gzip"
                | DecompressionResult.InsufficientSpace ->
                    error $"%s{nameof m.BinaryAttachment} is too big after decompression"
                | _ -> failwith "Invalid decompression result"
            | _ -> failwith "Absurd"

        if decompressedPayload.Length > maxPayloadLen then
            error "decompressed payload is too big"

        let message = { Topic = m.Destination.Name
                        ReplyTo =
                            if isNull m.ReplyTo || isNull m.ReplyTo.Name
                            then None
                            else Some m.ReplyTo.Name
                        ContentType = Option.ofObj m.HttpContentType
                        CorrelationId = Option.ofObj m.CorrelationId
                        SenderId = Option.ofObj m.SenderId
                        Payload = Bytes decompressedPayload
                      }
        let metadata = { SenderTimestamp = DateTimeOffset.FromUnixTimeMilliseconds m.SenderTimestamp
                         BrokerDiscardIndication = m.DiscardIndication
                       }
        message, metadata

    interface IConverter<IMessage, 'Incoming> with
        override _.Convert(m : IMessage) =
            if disposed then
                raise (ObjectDisposedException (nameof IncomingConverter))

            m
            |> convertToRawMessage
            |> f

        override _.Dispose() =
            if not disposed then
                disposed <- true
                decompressor.Dispose()

/// This class promotes function `f : 'Outgoing -> RawMessage`
/// to `IConverter<'Outgoing, IMessage>`.
/// The advantage is that type `RawMessage`
/// is normal F# record which is easier to work with than `IMessage`.
///
/// Payload length is limited by `maxPayloadLen` so that sent messages
/// aren't too big to be received.
///
/// `RawMessage` is encoded to `IMessage` in a such way that it can be decoded by `IncomingConverter`.
///
/// `f` shall raise if conversion is not possible.
type OutgoingConverter<'Outgoing>(maxPayloadLen : int, f : 'Outgoing -> RawMessage) =
    let mutable disposed = false
    let compressor = new Compressor(9)
    // Payload must have at least `minPayloadLenToCompress` bytes before we try to compress it.
    let minPayloadLenToCompress = 64
    let mutable buffer = Array.zeroCreate (1024 * 1024)  // This will grow if necessary.

    /// Raises exception when message `m` cannot be converted.
    ///
    /// `m` can be converted iff following conditions hold:
    /// - `m.Payload` has length less than or equal `maxPayloadLen`.
    let convertFromRawMessage (m : RawMessage) : IMessage =
        if m.Payload.Memory.Length > maxPayloadLen then
            failwith "Cannot convert outgoing message: decompressed payload is too big"

        let factory = ContextFactory.Instance
        let result = factory.CreateMessage()
        result.Destination <- factory.CreateTopic(m.Topic)
        m.ReplyTo |> Option.iter (fun x -> result.ReplyTo <- factory.CreateTopic(x))
        m.ContentType |> Option.iter (fun x -> result.HttpContentType <- x)
        m.CorrelationId |> Option.iter (fun x -> result.CorrelationId <- x)
        m.SenderId |> Option.iter (fun x -> result.SenderId <- x)

        if m.Payload.Memory.Length < minPayloadLenToCompress then
            result.BinaryAttachment <- m.Payload.Memory.ToArray()
        else
            // Ensure that `buffer` for compressed data is large enough.
            let minBufferLen = int (BitOperations.RoundUpToPowerOf2(uint m.Payload.Memory.Length))
            if buffer.Length < minBufferLen then
                buffer <- Array.zeroCreate minBufferLen

            let n = compressor.Compress(m.Payload.Span, Span buffer)

            // Use compression only if compressed payload is smaller.
            if n = 0 || n >= m.Payload.Memory.Length then
                result.BinaryAttachment <- m.Payload.Memory.ToArray()  // Avoid compression.
            else
                result.HttpContentEncoding <- "gzip"
                result.BinaryAttachment <- buffer[0 .. n - 1]
        result

    interface IConverter<'Outgoing, IMessage> with
        override _.Convert(m : 'Outgoing) =
            if disposed then
                raise (ObjectDisposedException (nameof OutgoingConverter))

            m
            |> f
            |> convertFromRawMessage

        override _.Dispose() =
            if not disposed then
                disposed <- true
                compressor.Dispose()

// -----------------------------------------------------------------------------------
// IPubSub implementation for Solace together with connect function
// -----------------------------------------------------------------------------------

/// Used to write received Solace messages to `inner`.
///
/// `ChannelWriter` which gets `IMessage`s,
/// translates them with function `f` to `'Incoming` and writes
/// translated messages to `inner`.
///
/// `f` shall raise exception if `IMessage` cannot be translated.
[<Sealed>]
type private IncomingMessageWriter<'Incoming>
    ( inner : ChannelWriter<'Incoming>,
      terminationReason : TaskCompletionSource,
      f : IMessage -> 'Incoming ) =

    inherit ChannelWriter<IMessage>()

    // `TryComplete` is used when PocketSolace closes the channel for received messages.
    // That happens after an error or when user calls `ISolace.Dispose`.
    // Closing the channel for received messages means we're terminating so we also set `terminationReason`.
    //
    // We actually set `terminationReason` before closing the channel. This is to ensure that
    // `terminationReason` contains correct termination reason. Eg. if we closed channel first
    // then background tasks would try to stop everything and one of those tasks could then
    // set another `terminationReason` before we manage to set correct one in this thread.
    override _.TryComplete(error) =
        if isNull error
        then terminationReason.TrySetResult()
        else terminationReason.TrySetException(error)
        |> ignore

        inner.TryComplete(error)

    // `TryWrite` is used when PocketSolace receives a message
    // and wants to add it into the channel for received messages.
    //
    // `TryWrite` must translate the message with `f` but must not throw exception.
    // When an exception is thrown we catch it and set it as `terminationReason`
    // and also use it to close the channel for received messages.
    //
    // When the message is not written by `inner.TryWrite` we
    // also set `terminationReason` and close the channel.
    //
    // So to summarize if the message is not written for any reason
    // (either because `f` raised or `inner.TryWrite` returned `false`).
    // the we set `terminationReason` and close the channel.
    override me.TryWrite(solaceMessage) =
        try
            let m = f solaceMessage
            let written = inner.TryWrite(m)
            if written then
                // Message is consumed after it's successfully written to `inner`.
                solaceMessage.Dispose()
            else
                me.TryComplete(Exception "Message channel full") |> ignore
            written
        with e ->
            me.TryComplete(e) |> ignore
            false

    override _.WaitToWriteAsync(token) = inner.WaitToWriteAsync(token)

    override _.WriteAsync(solaceMessage, token) = raise (NotImplementedException())

[<Sealed>]
type private SolacePubSub
    ( solace : ISolace,
      terminationReason : TaskCompletionSource,
      terminated : TaskCompletionSource
    ) =

    interface IPubSub with
        override _.TerminationReason = terminationReason.Task
        override _.Terminated = terminated.Task

        override _.DisposeAsync() =
            terminationReason.TrySetResult() |> ignore
            solace.DisposeAsync() |> ignore  // No need to wait for this since we already wait for `terminated`.
            ValueTask terminated.Task

let inline private deferAsync ([<InlineIfLambda>] f : unit -> ValueTask) =
    { new IAsyncDisposable with
        override _.DisposeAsync() = f () }


// TODO Think about clearer termination model. Eg. something like structured concurrency??
let connect (config : SolacePubSubConfig<'Incoming, 'Outgoing>) : Task<IPubSub> =
    Solace.initGlobalContextFactory SolLogLevel.Debug config.SolaceClientLogger

    let props = Solace.createSessionProperties ()
    props.Host <- config.Solace.Host
    props.UserName <- config.Solace.UserName
    props.Password <- config.Solace.Password
    props.VPNName <- config.Solace.Vpn
    props.ClientName <- config.ClientName
    props.GenerateSendTimestamps <- true

    backgroundTask {
        // Set when we start terminating or when we see `ISolace.TerminationReason`.
        let terminationReason = TaskCompletionSource()
        let terminated = TaskCompletionSource()  // Set after both `ISolace` and publishing task terminates.

        // For simplicity we don't bother disposing these. Instead we rely on GC to do it.
        let subConverter = config.SubConverter ()
        let pubConverter = config.PubConverter ()

        let writer = IncomingMessageWriter(config.SubChannel.Writer, terminationReason, subConverter.Convert)

        let! solace = Solace.connect config.PocketSolaceLogger props writer
        let mutable disposeSolace = true
        use _ = deferAsync (fun () ->
            if disposeSolace
            then solace.DisposeAsync()
            else ValueTask.CompletedTask)

        for s in config.Subscriptions do
            do! solace.Subscribe(s)

        let publishingTask = backgroundTask {
            while not terminationReason.Task.IsCompleted do
                try
                    let! outgoing = config.PubChannel.Reader.ReadAsync()
                    let m = pubConverter.Convert outgoing
                    do! solace.Send(m)
                with :? ChannelClosedException as e ->
                    terminationReason.TrySetException(Exception "Pub channel was closed") |> ignore
                    raise e  // For some reason we can't use `reraise ()` here.
        }

        // If `publishingTask` stops or we get disconnected from Solace then propagate/set termination reason.
        publishingTask.ContinueWith(Action<Task>(fun task ->
            config.PubSubLogger.LogInformation(task.Exception, "Publishing task stopped")
            if task.IsFaulted
            then terminationReason.TrySetException(task.Exception) |> ignore
            else
                if terminationReason.TrySetException(Exception("Publishing task stopped")) then
                    config.PubSubLogger.LogWarning("Publishing task stopped but no termination reason was set")
        )) |> ignore
        solace.Terminated.ContinueWith(Action<Task>(fun task ->
            config.PubSubLogger.LogInformation(task.Exception, "Solace stopped")
            if task.IsFaulted
            then terminationReason.TrySetException(task.Exception) |> ignore
            else
                if terminationReason.TrySetException(Exception("Solace terminated")) then
                    config.PubSubLogger.LogWarning("Solace terminated without exception")
        )) |> ignore

        // If we have termination reason then close both channels.
        terminationReason.Task.ContinueWith(Action<Task>(fun reason ->
            config.PubSubLogger.LogInformation(reason.Exception, "Termination reason was set")
            if reason.IsFaulted then
                config.SubChannel.Writer.TryComplete(reason.Exception) |> ignore
                config.PubChannel.Writer.TryComplete(reason.Exception) |> ignore
            else
                config.SubChannel.Writer.TryComplete() |> ignore
                config.PubChannel.Writer.TryComplete() |> ignore
            solace.DisposeAsync() |> ignore
        )) |> ignore

        // Mark `IPubSub` as terminated after both publishing task stops and we get disconnected from Solace.
        Task.WhenAll(solace.Terminated, publishingTask).ContinueWith(Action<Task>(fun task ->
            config.PubSubLogger.LogInformation("PubSub terminated")
            terminated.SetResult()
        )) |> ignore

        let pubSub = SolacePubSub(solace, terminationReason, terminated)
        disposeSolace <- false  // Now it's the responsibility of `pubSub` to dispose `solace`.

        return pubSub :> IPubSub
    }
