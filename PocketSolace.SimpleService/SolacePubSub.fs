module PocketSolace.SimpleService.SolacePubSub

open System
open System.Threading.Channels

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
