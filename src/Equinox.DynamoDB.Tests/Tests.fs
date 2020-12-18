module Tests

open Xunit
open Swensen.Unquote
open Equinox.DynamoDB
open Equinox.DynamoDB.Events

let maxBytes = 600
let decide = decideOperation maxBytes

let streamName = "stream1"

[<Fact>]
let ``Initial write`` () =
    let state = BatchState.Empty streamName
    let events = [| "event1"; "event2" |]
    let result = decide state events
    test <@ result = Decision.InsertFirstDocument events @>


[<Fact>]
let ``Append`` () =
    let batchInfo = {
        StreamName=streamName 
        CurrentOffset=int64(1) 
        PreviousOffset= None 
        TotalSize=0 
        Etag="abcd"
    }
    let state = BatchState.Events batchInfo
    let newEvents = [| "event3"; "event4" |]
    let result = decide state newEvents
    test <@ result = Decision.AppendToCurrent (newEvents, "abcd") @>


[<Fact>]
let ``Current is Full`` () =
    let batchInfo = {
        StreamName=streamName 
        CurrentOffset=int64(1) 
        PreviousOffset= None 
        TotalSize=maxBytes
        Etag="abcd"
    }
    let state = BatchState.Events batchInfo
    let newEvents = [| "eventBlob"; "eventBlob2" |]
    let result = decide state newEvents
    test <@ result = Decision.Overflow ([||], newEvents, "abcd") @>


[<Fact>]
let ``Inserts Straddle two Documents`` () =
    let batchInfo = {
        StreamName=streamName 
        CurrentOffset=int64(1) 
        PreviousOffset= None 
        TotalSize= maxBytes - "eventBlob".Length - 3
        Etag="abcd"
    }
    let state = BatchState.Events batchInfo
    let event1, event2 = "eventBlob", "eventBlob2"
    let newEvents = [| event1; event2 |]
    let result = decide state newEvents
    test <@ result = Decision.Overflow ([| event1 |], [| event2 |], "abcd") @>



[<Fact>]
let ``Insert empty events`` () = 
    let batchInfo = {
        StreamName=streamName 
        CurrentOffset=int64(1) 
        PreviousOffset= None 
        TotalSize= maxBytes - "eventBlob".Length - 3
        Etag="abcd"
    }
    let state = BatchState.Events batchInfo
    let result = decide state [||]
    test <@ result = Decision.NoOp @>