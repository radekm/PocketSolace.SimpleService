module PocketSolace.SimpleService.OutgoingConverterTest

open System
open System.Text

open NUnit.Framework

open PocketSolace.SimpleService.PubSub
open PocketSolace.SimpleService.SolacePubSub

[<Test>]
let ``minimal raw message is converted without using compression`` () =
    let rawMessage = { Topic = "dest"
                       ReplyTo = None
                       ContentType = None
                       CorrelationId = None
                       SenderId = None
                       Payload = Bytes ReadOnlyMemory.Empty
                     }
    use converter = new OutgoingConverter<_>(10, id) :> IConverter<_, _>
    let m = converter.Convert(rawMessage)

    Assert.AreEqual("dest", m.Destination.Name)
    Assert.IsNull(m.ReplyTo)
    Assert.IsNull(m.HttpContentType)
    Assert.IsNull(m.CorrelationId)
    Assert.IsNull(m.SenderId)
    Assert.IsNull(m.BinaryAttachment)  // Empty `BinaryAttachment` is translated to `null`.

    Assert.IsNull(m.HttpContentEncoding)


[<Test>]
let ``small raw message with all fields is converted without using compression`` () =
    let rawMessage = { Topic = "dest"
                       ReplyTo = Some "reply-dest"
                       ContentType = Some "type"
                       CorrelationId = Some "321"
                       SenderId = Some "xyz"
                       Payload = Bytes (Encoding.ASCII.GetBytes "yup" |> ReadOnlyMemory)
                     }
    use converter = new OutgoingConverter<_>(10, id) :> IConverter<_, _>
    let m = converter.Convert(rawMessage)

    Assert.AreEqual("dest", m.Destination.Name)
    Assert.AreEqual("reply-dest", m.ReplyTo.Name)
    Assert.AreEqual("type", m.HttpContentType)
    Assert.AreEqual("321", m.CorrelationId)
    Assert.AreEqual("xyz", m.SenderId)
    CollectionAssert.AreEqual([| 'y'B; 'u'B; 'p'B |], m.BinaryAttachment)

    Assert.IsNull(m.HttpContentEncoding)
