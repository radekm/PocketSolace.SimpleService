// `IPubSub` and and other types which are independent from Solace.
module PocketSolace.SimpleService.PubSub

open System
open System.Threading.Tasks

/// Wrapper around `ReadOnlyMemory<byte>` to use structural equality and hashing.
[<Struct; CustomEquality; NoComparison>]
type Bytes =
    | Bytes of ReadOnlyMemory<byte>

    member me.Memory =
        let (Bytes bytes) = me
        bytes

    member me.Span = me.Memory.Span

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

type IncomingMetadata =
    { SenderTimestamp : DateTimeOffset

      // Indicates that one or more of the previous messages was discarded by message broker.
      // The reason could be that the client is too slow so broker must discard some messages.
      BrokerDiscardIndication : bool
    }

/// `'T` should use structural equality.
type Message<'T> =
    { // Topics.
      Topic : string
      ReplyTo : string option

      // Metadata.
      ContentType : string option
      CorrelationId : string option
      SenderId : string option

      Payload : 'T
    }

type RawMessage = Message<Bytes>

/// `IConverter` for converting between different message formats.
///
/// Not thread-safe.
type IConverter<'From, 'To> =
    /// `Convert` can raise exception when conversion is impossible.
    /// `IConverter` can be used even after `Convert` raises exception.
    abstract Convert : 'From -> 'To

    inherit IDisposable

type IPubSub =
    abstract TerminationReason : Task
    abstract Terminated : Task

    inherit IAsyncDisposable
