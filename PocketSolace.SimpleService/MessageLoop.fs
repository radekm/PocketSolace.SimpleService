namespace PocketSolace.SimpleService

open System
open System.Numerics
open System.Threading
open System.Threading.Channels
open System.Threading.Tasks

open LibDeflateGzip
open Microsoft.Extensions.Logging
open PocketSolace
open SolaceSystems.Solclient.Messaging

/// Wrapper around `ReadOnlyMemory<byte>` to use structural equality and hashing.
[<Struct; CustomEquality; NoComparison>]
type Bytes =
    | Bytes of ReadOnlyMemory<byte>

    member me.Memory =
        let (Bytes bytes) = me
        bytes

    member me.Span = me.Memory.Span

    member me.Length = me.Memory.Length

    member me.ToArray() = me.Memory.ToArray()

    override me.Equals(other) =
        match other with
        | :? Bytes as other ->
            let (Bytes a) = me
            let (Bytes b) = other
            a.Span.SequenceEqual b.Span
        | _ -> false

    override me.GetHashCode() =
        let (Bytes bytes) = me
        bytes.Length

/// `'T` should use structural equality.
type Message<'T> =
    { // Topics.
      Topic : string
      ReplyTo : string option

      // Metadata.
      CorrelationId : string option
      SenderId : string option

      Payload : 'T
    }

    with
        member me.MapPayload<'U>(f : 'T -> 'U) = { Topic = me.Topic
                                                   ReplyTo = me.ReplyTo
                                                   CorrelationId = me.CorrelationId
                                                   SenderId = me.SenderId
                                                   Payload = f me.Payload
                                                 }

/// Payload contains bytes before compression.
type ByteMessage = Message<Bytes>

// -----------------------------------------------------------------------------------
// Config
// -----------------------------------------------------------------------------------

type SolaceConfig =
    { Host : string  // Contains host and port.
      UserName : string
      Password : string
      Vpn : string
    }

module MessageLoop =

    type IProcessor<'Incoming, 'Outgoing> =
        /// Must not raise exception.
        abstract Active : bool
        abstract Process : 'Incoming -> 'Outgoing list

    type Parameters<'In, 'Out> =
        { LoggerFactory : ILoggerFactory

          // ------------------------------------------------
          // ---- Solace session properties

          SolaceConfig : SolaceConfig
          /// Unique client name is formed by joining `SolaceClientNamePrefix` with random GUID.
          SolaceClientNamePrefix : string

          // ------------------------------------------------
          // ---- Receiving and sending Solace messages

          SolaceSubscriptions : string list
          // NOTE: Size limit for compressed messages is determined by Solace broker.
          //       Too big messages can't be transferred.
          //       So we check only decompressed messages.
          //       This prevents decompression bombs.
          MaxDecompressedPayloadLen : int
          /// Used to convert messages received from Solace before putting them into `Inputs` channel.
          SolaceInConverter : IncomingMetadata * ByteMessage -> 'In list
          /// Used to convert messages from `Processor` before sending them to Solace.
          SolaceOutConverter : 'Out -> ByteMessage list

          // ------------------------------------------------

          /// Messages read from this channel are given to `Processor`.
          /// Messages received from Solace are written to this channel.
          /// Note that other parts of the application can also write messages to this channel
          /// if they want to pass messages to processor.
          Inputs : Channel<'In>
          // NOTE: `Processor` is intentionally generic in its input and output types.
          //       This should simplify testing because serialization logic could be moved
          //       to `SolaceInConverter` and `SolaceOutConverter`.
          //       Otherwise nothing prevents processor from directly producing byte messages.
          Processor : IProcessor<'In, 'Out>
        }

    /// Like defer from Zig or Go.
    let inline private defer ([<InlineIfLambda>] f : unit -> unit) =
        { new IDisposable with
            override _.Dispose() = f () }

    [<Literal>]
    let private Gzip = "gzip"

    // CONSIDER: Skipping messages with invalid content encoding or too big decompressed payload or invalid gzip.
    //           Because for some applications it's better to process at least some messages instead of none.

    /// `run parameters` does following:
    /// (1) Connects to Solace and subscribes to `parameters.SolaceSubscriptions` topics.
    ///     Topics are subscribed one by one in the same order as in the list `parameters.SolaceSubscriptions`.
    /// (2) At the same time spawns two tasks message pump and processing loop.
    ///     - Message pump processes messages from Solace:
    ///       (a) It decompresses their payload,
    ///       (b) then uses `parameters.SolaceInConverter` to convert them to `'In`s
    ///       (c) and then writes those into the channel `parameters.Inputs`.
    ///    - Processing loop processes inputs from the channel `parameters.Inputs` and sends messages to Solace.
    ///      Before processing a message from `parameters.Inputs` it checks whether `parameters.Processor`
    ///      is active. If not it stops immediately. Otherwise it does following:
    ///      (a) It reads an input from `parameters.Inputs`,
    ///      (b) then uses `parameters.Processor` to convert the input to `'Out`s,
    ///      (c) then uses `parameters.SolaceOutConverter` to convert each output to byte message,
    ///      (d) then payload of each byte message is compressed
    ///      (e) and compressed messages are sent.
    /// (3) Monitors both spawned tasks. If one stops then cancels the other.
    ///     The task returned by `run` terminates only AFTER all of the following conditions are true:
    ///     - Both spawned tasks complete,
    ///     - connection to Solace is closed
    ///     - and `parameters.SolaceInConverter`, `parameters.SolaceOutConverter`, `parameters.Inputs`
    ///       and `parameters.Processor` are no longer used.
    ///
    /// NOTES:
    /// - Payload is compressed only if it's big enough.
    /// - When receiving a message and before sending a message
    ///   the size of the decompressed payload is checked.
    ///   It must be smaller than `parameters.MaxDecompressedPayloadLen`.
    ///
    /// Typical events which cause `run` to stop:
    /// - Client disconnects from Solace broker.
    /// - `parameters.Processor` becomes inactive.
    /// - `parameters.SolaceInConverter` or `parameters.SolaceOutConverter` or `parameters.Processor.Process`
    ///    raise an exception.
    /// - `parameters.Inputs` channel is completed.
    /// - Message with invalid content encoding or payload is received.
    /// - Message with too big decompressed payload is received.
    /// - Message with too big decompressed payload is sent.
    let run (parameters : Parameters<'In, 'Out>) = backgroundTask {
        let solaceClientLogger = parameters.LoggerFactory.CreateLogger("Solace Client")
        let pocketSolaceLogger = parameters.LoggerFactory.CreateLogger("Pocket Solace")
        let messageLoopLogger = parameters.LoggerFactory.CreateLogger("Message Loop")

        // Client name must be unique -- otherwise client won't be able to connect to broker.
        let uniqueClientName = parameters.SolaceClientNamePrefix + "-" + Guid.NewGuid().ToString()
        use _ = defer (fun () -> messageLoopLogger.LogInformation($"Message loop for %s{uniqueClientName} is stopping"))

        Solace.initGlobalContextFactory SolLogLevel.Debug solaceClientLogger

        let props = Solace.createSessionProperties ()
        props.Host <- parameters.SolaceConfig.Host
        props.UserName <- parameters.SolaceConfig.UserName
        props.Password <- parameters.SolaceConfig.Password
        props.VPNName <- parameters.SolaceConfig.Vpn
        props.ClientName <- uniqueClientName

        let receiveChannelCapacity = 1024
        use cts = new CancellationTokenSource()
        let token = cts.Token
        use! solace = Solace.connect pocketSolaceLogger props receiveChannelCapacity
        for subscription in parameters.SolaceSubscriptions do
            do! solace.Subscribe(subscription)

        let terminationReason = TaskCompletionSource()

        let messagePump = backgroundTask {
            use decompressor = new Decompressor()
            let extraSpace = 128  // Decompressor may need slightly more space.
            let buffer : byte[] = Array.zeroCreate (parameters.MaxDecompressedPayloadLen + extraSpace)
            while not token.IsCancellationRequested do
                let! metadata, rawMessage = solace.Received.ReadAsync(token)
                let decompressedPayload =
                    match rawMessage.ContentEncoding with
                    | None -> ReadOnlyMemory rawMessage.Payload
                    | Some Gzip ->
                        let result, read, written = decompressor.Decompress(rawMessage.Payload, buffer)
                        match result with
                        | DecompressionResult.Success ->
                            if read < rawMessage.Payload.Length then
                                failwith "Payload contains more than one gzip member"
                            ReadOnlyMemory buffer[0 .. written - 1]
                        | DecompressionResult.BadData ->
                            failwith "Payload contains invalid gzip"
                        | DecompressionResult.InsufficientSpace ->
                            failwith "Decompressed payload is too big (decompression interrupted)"
                        | _ -> failwith "Invalid decompression result"
                    | Some encoding -> failwith $"Invalid content encoding: %s{encoding}"

                if decompressedPayload.Length > parameters.MaxDecompressedPayloadLen then
                    failwith "Decompressed payload is too big"

                let byteMessage = { Topic = rawMessage.Topic
                                    ReplyTo = rawMessage.ReplyTo
                                    CorrelationId = rawMessage.CorrelationId
                                    SenderId = rawMessage.SenderId
                                    Payload = Bytes decompressedPayload
                                  }
                let inputs = parameters.SolaceInConverter (metadata, byteMessage)
                for input in inputs do
                    do! parameters.Inputs.Writer.WriteAsync(input, token)
        }
        // Handle termination of `messagePump`.
        let messagePump = backgroundTask {
            use _ = defer (fun () -> cts.Cancel())
            try
                do! messagePump
                terminationReason.TrySetResult() |> ignore
                messageLoopLogger.LogInformation($"Message pump for %s{uniqueClientName} stopped")
            with e ->
                terminationReason.TrySetException(e) |> ignore
                messageLoopLogger.LogInformation(e, $"Message pump for %s{uniqueClientName} stopped with exception")
        }

        let processingLoop = backgroundTask {
            let compressor = new Compressor(9)
            // Payload must have at least `minPayloadLenToCompress` bytes before we try to compress it.
            let minPayloadLenToCompress = 64
            let mutable buffer = Array.zeroCreate (1024 * 1024)  // This will grow if necessary.

            while parameters.Processor.Active && not token.IsCancellationRequested do
                let! input = parameters.Inputs.Reader.ReadAsync(token)
                let outputs = parameters.Processor.Process(input)
                for output in outputs do
                    let byteMessages = parameters.SolaceOutConverter(output)
                    for byteMessage in byteMessages do
                        if byteMessage.Payload.Length > parameters.MaxDecompressedPayloadLen then
                            failwith "Payload of outgoing message is too big"
                        let encoding, payload =
                            if byteMessage.Payload.Length < minPayloadLenToCompress then
                                None, byteMessage.Payload.ToArray()
                            else
                                // Ensure that `buffer` for compressed data is large enough.
                                // NOTE: Compressor from libdeflate needs extra space but we don't
                                //       explicitly add it to buffer size. Instead we make buffer
                                //       large enough to hold decompressed data and hope that compression
                                //       ratio will be good enough that some extra space will be left.
                                //       If compression ratio is not good enough and there's not enough
                                //       extra space left then we don't compress data.
                                let minBufferLen =
                                    int (BitOperations.RoundUpToPowerOf2(uint byteMessage.Payload.Length))
                                if buffer.Length < minBufferLen then
                                    buffer <- Array.zeroCreate minBufferLen

                                let n = compressor.Compress(byteMessage.Payload.Span, Span buffer)

                                // Use compression only if compressed payload is smaller.
                                if n = 0 || n >= byteMessage.Payload.Length then
                                    None, byteMessage.Payload.ToArray()  // Avoid compression.
                                else
                                    Some Gzip, buffer[0 .. n - 1]

                        let rawMessage = { Topic = byteMessage.Topic
                                           ReplyTo = byteMessage.ReplyTo
                                           ContentType = None
                                           ContentEncoding = encoding
                                           CorrelationId = byteMessage.CorrelationId
                                           SenderId = byteMessage.SenderId
                                           Payload = payload
                                         }

                        // Sending can take a long time and can't be interrupted so before we start sending we
                        // always check whether cancellation was requested.
                        token.ThrowIfCancellationRequested()
                        do! solace.Send(rawMessage)
        }
        // Handle termination of `processingLoop`.
        let processingLoop = backgroundTask {
            use _ = defer (fun () -> cts.Cancel())
            try
                do! processingLoop
                terminationReason.TrySetResult() |> ignore
                messageLoopLogger.LogInformation($"Processing loop for %s{uniqueClientName} stopped")
            with e ->
                terminationReason.TrySetException(e) |> ignore
                messageLoopLogger.LogInformation(e, $"Processing loop for %s{uniqueClientName} stopped with exception")
        }

        let! _ = Task.WhenAll(messagePump, processingLoop)

        // NOTE: Safety of disposal of `cts` and `solace`.
        //       `cts` and `solace` are only used in `messagePump` and `processingLoop`.
        //       At this point both tasks are complete
        //       (provided strange condition like OutOfMemoryException hasn't happened).
        //       This means we can safely dispose `cts` and `solace`.

        // We want to return or raise initial cause of termination.
        // Because user of `run` needs to know the real reason behind termination.
        //
        // When the processor becomes inactive `terminationReason` contains normal result.
        // Otherwise it contains exception from either message pump or processing loop which was caught first.
        //
        // `Task.WhenAll` is not suited because it returns exception from the leftmost parameter
        // and gives subtypes of `OperationCanceledException` lower priority.
        do! terminationReason.Task
    }
