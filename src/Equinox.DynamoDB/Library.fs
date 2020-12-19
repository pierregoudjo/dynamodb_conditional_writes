namespace Equinox.DynamoDB

type Event = string

type BatchInfo = {
    StreamName: string
    CurrentOffset: int64
    PreviousOffset: int64 option
    TotalSize: int
    Etag: string
}

type Empty = BatchInfo

type StreamState =
    | Empty of streamName:string
    | Events of currentBatchInfo: BatchInfo


type Decision =
    | NoOp
    | InsertFirstDocument of events: Event[] * streamName: string
    | AppendToCurrent of events: Event[] * currentBatch: BatchInfo
    | Overflow of appendToCurrent: Event[] * insertNext: Event[] * batch: BatchInfo


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
                        | StreamState.Empty streamName-> InsertFirstDocument (events, streamName)
                        | StreamState.Events info ->
                            match split info.TotalSize [||] events with
                            | buffer, [||] -> AppendToCurrent (buffer, info)
                            | buffer, overflow -> Overflow (buffer, overflow, info)
module Effects = 
    open FSharp.AWS.DynamoDB
    open System

    type Command = 
        | Idle
        | Start of events: Event[] * streamName: string
        | Update of onCurrent: Event[] * info: BatchInfo
        | Next of nextEvents: Event[] * previousBatchInfo: BatchInfo

    let matchDecision (decision) : Command[]= 
        match decision with
        | NoOp -> [|Idle|]
        | InsertFirstDocument (events, streamName) -> [|Start (events, streamName)|]
        | AppendToCurrent (events, batch) -> [|Update (events, batch)|]
        | Overflow (onCurrent, onNext, batch) -> [|Update (onCurrent, batch); Next (onNext, batch)|]

    type Entry =
       {
          [<HashKey>]
          EntryID: string
          [<RangeKey>]
          Offset: int64
          PreviousOffset: int64 option
          
          Etag: string
          Events: string array
          LastEventTimeStamp: DateTimeOffset option
       }

    let applyCommand (tableContext: TableContext<Entry>) (command) = 
        match command with
        | Idle -> ignore "No action"
        | Start (events, stream) ->
            let entry = {
                EntryID = stream
                Offset = int64(0)
                Etag = Guid.NewGuid().ToString()
                PreviousOffset = None
                Events = events
                LastEventTimeStamp = Some DateTimeOffset.Now 
            }
            ignore (tableContext.PutItem(entry))
        | Update (events, info) -> 
            ignore (
                tableContext.UpdateItem(TableKey.Combined(info.StreamName, info.CurrentOffset), 
                    <@ fun r -> {r with Events = Array.append r.Events events } @>, 
                    precondition = <@ fun r -> r.Etag = info.Etag @>)
            )

        | Next (next, info) ->
            ignore (tableContext.PutItem({
                EntryID = info.StreamName
                Offset = info.CurrentOffset + int64(1)
                Etag = Guid.NewGuid().ToString()
                PreviousOffset = Some info.CurrentOffset
                Events = next
                LastEventTimeStamp = Some DateTimeOffset.Now 
            }))

    let createtableContext client tableName= TableContext.Create<Entry>(client, tableName)

