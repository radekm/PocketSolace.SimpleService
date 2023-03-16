module PocketSolace.SimpleService.IncomingConverterTest

open System
open System.Text

open LibDeflateGzip
open NUnit.Framework

open PocketSolace.SimpleService.PubSub
open PocketSolace.SimpleService.SolacePubSub

let createMessage () =
    SolaceSystems.Solclient.Messaging.ContextFactory.Instance.CreateMessage()

let createTopic (topic : string) =
    SolaceSystems.Solclient.Messaging.ContextFactory.Instance.CreateTopic(topic)

let compress (bytes : byte[]) =
    use compressor = new Compressor(12)
    let output = Array.zeroCreate 1024
    let n = compressor.Compress(ReadOnlySpan bytes, Span output)
    if n = 0 then
        failwith "Compression failed"  // Buffer `output` is probably not big enough.
    output[0 .. n - 1]

module OkMessages =
    [<Test>]
    let ``convert minimal IMessage`` () =
        use m = createMessage ()
        m.SenderTimestamp <- 1
        m.Destination <- createTopic "foo"

        let expected =
            let rawMessage = { Topic = "foo"
                               ReplyTo = None
                               ContentType = None
                               CorrelationId = None
                               SenderId = None
                               Payload = Bytes ReadOnlyMemory.Empty }
            let metadata = { SenderTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(1)
                             BrokerDiscardIndication = false }
            rawMessage, metadata

        use converter = new IncomingConverter<_>(10, id) :> IConverter<_, _>
        let actual = converter.Convert(m)

        Assert.AreEqual(expected, actual)

    [<Test>]
    let ``convert IMessage where payload is not compressed`` () =
        use m = createMessage ()
        m.SenderTimestamp <- 2
        m.Destination <- createTopic "foo"
        m.ReplyTo <- createTopic "bar"
        m.HttpContentType <- "some content"
        m.CorrelationId <- "123"
        m.SenderId <- "service-8"
        m.BinaryAttachment <- [| 'h'B; 'i'B |]

        let expected =
            let rawMessage = { Topic = "foo"
                               ReplyTo = Some "bar"
                               ContentType = Some "some content"
                               CorrelationId = Some "123"
                               SenderId = Some "service-8"
                               Payload = Bytes (Encoding.ASCII.GetBytes "hi" |> ReadOnlyMemory) }
            let metadata = { SenderTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(2)
                             BrokerDiscardIndication = false }
            rawMessage, metadata

        use converter = new IncomingConverter<_>(10, id) :> IConverter<_, _>
        let actual = converter.Convert(m)

        Assert.AreEqual(expected, actual)

    [<Test>]
    let ``convert IMessage where payload is compressed`` () =
        use m = createMessage ()
        m.SenderTimestamp <- 2
        m.Destination <- createTopic "foo"
        m.ReplyTo <- createTopic "bar"
        m.HttpContentType <- "some content"
        m.CorrelationId <- "123"
        m.SenderId <- "service-8"
        m.HttpContentEncoding <- "gzip"
        m.BinaryAttachment <- compress (Encoding.ASCII.GetBytes "Hello world!!")

        let expected =
            let rawMessage = { Topic = "foo"
                               ReplyTo = Some "bar"
                               ContentType = Some "some content"
                               CorrelationId = Some "123"
                               SenderId = Some "service-8"
                               Payload = Bytes (Encoding.ASCII.GetBytes "Hello world!!" |> ReadOnlyMemory) }
            let metadata = { SenderTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(2)
                             BrokerDiscardIndication = false }
            rawMessage, metadata

        use converter = new IncomingConverter<_>(13, id) :> IConverter<_, _>
        let actual = converter.Convert(m)

        Assert.AreEqual(expected, actual)

module BadMessages =
    [<Test>]
    let ``convert IMessage where SenderTimestamp is missing`` () =
        use m = createMessage ()
        m.Destination <- createTopic "foo"

        use converter = new IncomingConverter<_>(10, id) :> IConverter<_, _>
        StringAssert.Contains(
            nameof m.SenderTimestamp,
            Assert.Throws(fun () -> converter.Convert(m) |> ignore).Message)

    [<Test>]
    let ``convert IMessage where Destination is missing`` () =
        use m = createMessage ()
        m.SenderTimestamp <- 2

        use converter = new IncomingConverter<_>(10, id) :> IConverter<_, _>
        StringAssert.Contains(
            $"%s{nameof m.Destination}.%s{nameof m.Destination.Name}",
            Assert.Throws(fun () -> converter.Convert(m) |> ignore).Message)

    [<Test>]
    let ``convert IMessage where HttpContentEncoding has unknown value`` () =
        use m = createMessage ()
        m.SenderTimestamp <- 2
        m.Destination <- createTopic "foo"
        m.HttpContentEncoding <- "xxx"

        use converter = new IncomingConverter<_>(10, id) :> IConverter<_, _>
        StringAssert.Contains(
            nameof m.HttpContentEncoding,
            Assert.Throws(fun () -> converter.Convert(m) |> ignore).Message)

    [<Test>]
    let ``convert IMessage with too big BinaryAttachment`` () =
        use m = createMessage ()
        m.SenderTimestamp <- 2
        m.Destination <- createTopic "foo"
        m.BinaryAttachment <- Encoding.ASCII.GetBytes "0123456789x"

        use converter = new IncomingConverter<_>(10, id) :> IConverter<_, _>
        StringAssert.Contains(
            "decompressed payload is too big",
            Assert.Throws(fun () -> converter.Convert(m) |> ignore).Message)

    [<Test>]
    let ``convert IMessage with too big BinaryAttachment after decompression`` () =
        let maxPayloadLen = 2000

        use m = createMessage ()
        m.SenderTimestamp <- 2
        m.Destination <- createTopic "foo"
        m.HttpContentEncoding <- "gzip"
        m.BinaryAttachment <- compress (Array.create (maxPayloadLen + 1) 'a'B)

        Assert.LessOrEqual(m.BinaryAttachment.Length, maxPayloadLen)  // Check size of compressed payload.

        use converter = new IncomingConverter<_>(maxPayloadLen, id) :> IConverter<_, _>
        StringAssert.Contains(
            "decompressed payload is too big",
            Assert.Throws(fun () -> converter.Convert(m) |> ignore).Message)

    [<Test>]
    let ``convert IMessage with BinaryAttachment which cannot be decompressed`` () =
        use m = createMessage ()
        m.SenderTimestamp <- 2
        m.Destination <- createTopic "foo"
        m.HttpContentEncoding <- "gzip"
        m.BinaryAttachment <- Array.init 1000 (fun i -> byte i)  // Not gzip data.

        use converter = new IncomingConverter<_>(1024, id) :> IConverter<_, _>
        StringAssert.Contains(
            "invalid gzip",
            Assert.Throws(fun () -> converter.Convert(m) |> ignore).Message)
