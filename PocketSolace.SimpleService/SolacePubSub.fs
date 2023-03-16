module PocketSolace.SimpleService.SolacePubSub

open System
open System.Threading.Channels

open LibDeflateGzip
open Microsoft.Extensions.Logging
open SolaceSystems.Solclient.Messaging

open PubSub

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

      MaxPayloadLen : int

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