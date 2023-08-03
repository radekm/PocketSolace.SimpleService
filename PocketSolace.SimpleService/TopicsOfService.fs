namespace PocketSolace.SimpleService

open System

type ReplyContext = { CorrelationId : CorrelationId
                      ReplyTo : string }

module TopicsOfService =
    let private checkTopicPart (name : string) (part : string) =
        if isNull part then
            failwith $"%s{name} must not be null"
        if part = "" then
            failwith $"%s{name} must not be empty"
        part
        |> String.iter (fun c ->
            if c = '/' then
                failwith $"%s{name} must not contain slashes"
            if c = '*' then
                failwith $"%s{name} must not contain stars"
            if c = '>' then
                failwith $"%s{name} must not contain greater than signs"
            if c = '#' then
                failwith $"%s{name} must not contain hashes"
            if Char.IsWhiteSpace c then
                failwith $"%s{name} must not contain whitespace")

    let checkServiceName (serviceName : string) =
        if isNull serviceName then
            failwith "Service name must not be null"
        let parts = serviceName.Split '/'
        if parts.Length <> 3 then
            failwith "Service name must have 3 parts"
        for part in parts do
            checkTopicPart "Service name part" part

    let checkServiceInstanceId (serviceInstanceId : string) = checkTopicPart "Service instance id" serviceInstanceId

    let checkUsername (username : string) = checkTopicPart "Username" username

    /// Tries readers one after another until one succeeds. Then returns its result.
    let findAndApplyReader
        (byteMessage : ByteMessage)
        (readers : list<ByteMessage -> 'T option>) : 'T =

        readers
        |> List.tryPick (fun read -> read byteMessage)
        |> function
            | None -> failwith $"No reader found for message received on topic %s{byteMessage.Topic}"
            | Some result -> result

    type TopicsForClient<'Feed, 'Command, 'Reply> internal
        ( feedTopic : string,
          commandTopicPrefix : string,
          replyTopicPrefix : string,
          clientServiceName : string,
          clientServiceInstanceId : string,
          clientUsername : string,
          readFeed : ReadOnlyMemory<byte> -> 'Feed,
          writeCommand : 'Command -> ReadOnlyMemory<byte>,
          readReply : ReadOnlyMemory<byte> -> 'Reply ) =

        let suffix = $"%s{clientServiceName}/%s{clientServiceInstanceId}/%s{clientUsername}"
        let commandTopic = $"%s{commandTopicPrefix}%s{suffix}"
        let replyTopic = $"%s{replyTopicPrefix}%s{suffix}"

        member _.FeedSubscription = feedTopic
        member val FeedReader : ByteMessage -> option<'Feed * SenderId> = fun byteMessage ->
            if byteMessage.Topic = feedTopic then
                match byteMessage.SenderId with
                | None -> failwith $"Feed message in topic %s{byteMessage.Topic} is missing sender id"
                | Some senderId -> Some (readFeed byteMessage.Payload.Memory, senderId)
            else None

        member val CommandWriter : 'Command -> CorrelationId option -> ByteMessage = fun command correlationId ->
            { Topic = commandTopic
              CorrelationId = correlationId
              SenderId = None
              Payload = Bytes (writeCommand command) }

        member _.ReplySubscription = replyTopic
        member val ReplyReader : ByteMessage -> option<'Reply * CorrelationId> = fun byteMessage ->
            if byteMessage.Topic.StartsWith replyTopicPrefix then
                match byteMessage.CorrelationId with
                | None -> failwith $"Reply message in topic %s{byteMessage.Topic} is missing correlation id"
                | Some correlationId -> Some (readReply byteMessage.Payload.Memory, correlationId)
            else None

    type TopicsForServiceImplementation<'Feed, 'Command, 'Reply> internal
        ( feedTopic : string,
          commandTopicPrefix : string,
          replyTopicPrefix : string,
          serviceName : string,
          serviceInstanceId : string,
          username : string,
          writeFeed : 'Feed -> ReadOnlyMemory<byte>,
          readCommand : ReadOnlyMemory<byte> -> 'Command,
          writeCommand : 'Command -> ReadOnlyMemory<byte>,
          writeReply : 'Reply -> ReadOnlyMemory<byte> ) =

        let commandTopic = $"%s{commandTopicPrefix}%s{serviceName}/%s{serviceInstanceId}/%s{username}"

        member val FeedWriter : 'Feed -> ByteMessage = fun feed ->
            { Topic = feedTopic
              CorrelationId = None
              SenderId = Some serviceInstanceId
              Payload = Bytes (writeFeed feed) }

        member val CommandSubscription = $"%s{commandTopicPrefix}*/*/*/*/*"
        member val CommandReader : ByteMessage -> option<'Command * ReplyContext option> = fun byteMessage ->
            if byteMessage.Topic.StartsWith commandTopicPrefix then
                let command = readCommand byteMessage.Payload.Memory
                match byteMessage.CorrelationId with
                | None -> Some (command, None)
                | Some correlationId ->
                    let replyContext =
                        { CorrelationId = correlationId
                          ReplyTo = replyTopicPrefix + byteMessage.Topic.Substring(commandTopicPrefix.Length) }
                    Some (command, Some replyContext)
            else None
        /// Useful if service wants to send command to itself.
        member val CommandWriter : 'Command -> CorrelationId option -> ByteMessage = fun command correlationId ->
            { Topic = commandTopic
              CorrelationId = correlationId
              SenderId = None
              Payload = Bytes (writeCommand command) }

        member val ReplyWriter : 'Reply -> ReplyContext -> ByteMessage = fun reply replyContext ->
            if replyContext.ReplyTo.StartsWith replyTopicPrefix then
                { Topic = replyContext.ReplyTo
                  CorrelationId = Some replyContext.CorrelationId
                  SenderId = None
                  Payload = Bytes (writeReply reply) }
            else failwith $"ReplyTo must start with %s{replyTopicPrefix}"

// CONSIDER: Splitting command topic into two topics.
//           One with commands without reply, other with requests with reply.
//           Commands don't need correlation id requests do.

/// This class implements a convention for structuring topics of services.
///
/// Each service has a name which consists of three parts separated by slash.
/// Eg. `exchange/trades/db`.
///
/// Each service publishes heartbeat, updates and optionally snapshots to feed topic `[service-name]/feed`.
/// - Updates are messages describing how data managed by service changed.
///   Updates are published only on change.
/// - Heartbeat is a message which signals that service is alive.
///   It's published periodically (usual interval between heartbeats can be 1 to 30 seconds).
/// - Snapshot can be used by other services to get current data
///   without asking the service. Snapshots are especially useful when
///   feed is recorded and analyzed. Thanks to the snapshots analyzers don't have to read all updates
///   from the beginning instead they can find the nearest snapshot and start from there.
///   Snapshot are published periodically (usual interval between snapshots is 30 seconds to several hours).
///   Frequent snapshots can replace heartbeat.
///
/// Some services accept commands on command topics
/// `[service-name]/command/[client-service-name]/[client-service-instance-id]/[client-username]`.
/// Different clients send commands to different topics. Each command topic contains `client-username`.
/// Authenticity of `client-username` can be enforced by Solace ACL Profiles.
/// Thanks to this recorded commands may be inspected to see which user performed which action.
///
/// Some commands may have associated reply. Replies are sent to topics
/// `[service-name]/reply/[client-service-name]/[client-service-instance-id]/[client-username]`.
/// Reply topic is derived from command topic by replacing prefix `[service-name]/command/`
/// by `[service-name]/reply/`.
///
/// NOTES:
/// - Useful property of our topic naming scheme is that the first four parts of each topic determine message type,
///   serializer and deserializer.
/// - Messages on feed topic carry sender id header with `service-instance-id`.
///   This can be used to detect restart of a service.
/// - Multiple instances of one service share the same feed topic.
///   Messages can be differentiated by `service-instance-id`.
/// - On the other hand multiple instances of one service use different command topics
///   and different reply topics. Using different reply topics improves performance
///   because service isn't subscribed for replies for other instances.
///   Using different command topics allows auditing and thanks to that reply topic
///   can be derived from command topic.
/// - When command is sent with correlation id then `CommandReader` returns `ReplyContext`.
/// - This class can also be used for client applications which are like services but don't publish feed
///   and don't receive commands.
type TopicsOfService<'Feed, 'Command, 'Reply>
    ( serviceName : string,
      readFeed : ReadOnlyMemory<byte> -> 'Feed,
      writeFeed : 'Feed -> ReadOnlyMemory<byte>,
      readCommand : ReadOnlyMemory<byte> -> 'Command,
      writeCommand : 'Command -> ReadOnlyMemory<byte>,
      readReply : ReadOnlyMemory<byte> -> 'Reply,
      writeReply : 'Reply -> ReadOnlyMemory<byte> ) =

    do TopicsOfService.checkServiceName serviceName

    let feedTopic = $"%s{serviceName}/feed"
    let commandTopicPrefix = $"%s{serviceName}/command/"
    let replyTopicPrefix = $"%s{serviceName}/reply/"

    member _.TopicsForClient
        ( clientServiceName : string,
          clientServiceInstanceId : string,
          clientUsername : string ) =

        TopicsOfService.checkServiceName clientServiceName
        TopicsOfService.checkServiceInstanceId clientServiceInstanceId
        TopicsOfService.checkUsername clientUsername

        TopicsOfService.TopicsForClient(
            feedTopic, commandTopicPrefix, replyTopicPrefix,
            clientServiceName, clientServiceInstanceId, clientUsername,
            readFeed, writeCommand, readReply)

    member _.TopicsForServiceImplementation(serviceInstanceId : string, username : string) =
        TopicsOfService.checkServiceInstanceId serviceInstanceId
        TopicsOfService.checkUsername username

        TopicsOfService.TopicsForServiceImplementation(
            feedTopic, commandTopicPrefix, replyTopicPrefix,
            serviceName, serviceInstanceId, username,
            writeFeed, readCommand, writeCommand, writeReply)
