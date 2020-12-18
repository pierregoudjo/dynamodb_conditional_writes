namespace Equinox.DynamoDB

type Event = string

type StreamState =
    | Empty
    | Events of totalSize : int * etag: string

type Decision =
    | NoOp
    | InsertFirstDocument of Event[]
    | AppendToCurrent of events: Event[] * etag: string
    | Overflow of appendToCurrent: Event[] * insertNext: Event[] * etag: string

module Events = 
    let decideOperation (maxBytes : int) (currentState : StreamState) (newEvents : Event[]) =
        let rec split currentSize buffer (events : string[]) : string[] * string[] =
            let h, t = Array.head events, Array.tail events
            let thisSize = h.Length
            if currentSize + thisSize > maxBytes then (buffer, events)
            elif Array.isEmpty t then (Array.append buffer [|h|], [||])
            else split (currentSize + thisSize) (Array.append buffer [|h|]) (Array.tail events)

        match newEvents with
            | [||] -> NoOp
            | events -> match currentState with
                        | StreamState.Empty -> InsertFirstDocument events
                        | StreamState.Events (currentSize,etag) when currentSize >= maxBytes -> Overflow ([||], events, etag)
                        | StreamState.Events (currentSize,etag) ->
                            match split currentSize [||] events with
                            | buffer, [||] -> AppendToCurrent (buffer, etag)
                            | buffer, overflow -> Overflow (buffer, overflow, etag)

module Effects = 
    open FSharp.AWS.DynamoDB
    open System

    type Entry =
       {
          [<HashKey>]
          EntryID: string
          
          Etag: string
          Events: string array
          LastEventTimeStamp: DateTimeOffset option
       }

    let generateDynamoEffect (tableContext: TableContext<Entry>) (entryID) (decision) = 
        match decision with
        | NoOp -> ignore "no value"
        | InsertFirstDocument events -> 
            ignore (tableContext.PutItem({
                EntryID = entryID
                Etag = System.Guid.NewGuid().ToString()
                Events = events
                LastEventTimeStamp = Some DateTimeOffset.Now 
            }))
        | AppendToCurrent (events, etag) -> ignore (tableContext.UpdateItem(TableKey.Hash(entryID), <@ fun r -> {r with Events = Array.append r.Events events } @>, precondition = <@ fun r -> r.Etag = etag @>))
        | Overflow(appendToCurrent, insertNext, etag) -> failwith "Not Implemented"

    let createtableContext client tableName= TableContext.Create<Entry>(client, tableName)
