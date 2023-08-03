namespace PocketSolace.SimpleService.SampleDbService

open System

open PocketSolace.SimpleService

// This file contains public API of db service.
//
// The service has feed where it sends
// - heartbeat
// - and info about updated trades.
//
// The service supports two commands:
// - Command which returns all trades stored in db. This command has reply.
// - Command which is used to add or update trade in db.

type TradeId = string

type Trade = { Id : TradeId
               Price : decimal
               Volume : decimal }

type Feed =
    | Heartbeat
    | UpdatedTrade of Trade

type Command =
    // With reply.
    | GetAllTrades
    // Without reply.
    | AddOrUpdateTrade of Trade

type Reply =
    | AllTrades of Trade list

// Functions for (de)serializing messages.
// Not part of public API.
module private Json =
    open System.Text.Json
    open System.Text.Json.Serialization

    // There's no need for `WithUnionNamedFields` since unions have usually only a few fields.
    let private options = JsonFSharpOptions.Default().ToJsonSerializerOptions()

    let read<'T> (bytes : ReadOnlyMemory<byte>) : 'T = JsonSerializer.Deserialize(bytes.Span, options)
    let write<'T> (x : 'T) = ReadOnlyMemory (JsonSerializer.SerializeToUtf8Bytes(x, options))

type Topics() =
    inherit TopicsOfService<Feed, Command, Reply>(
        "test/trades/db", Json.read, Json.write, Json.read, Json.write, Json.read, Json.write)
