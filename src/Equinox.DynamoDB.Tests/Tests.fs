module Tests

open Xunit
open Swensen.Unquote
open Equinox.DynamoDB
open Equinox.DynamoDB.Events
open Equinox.DynamoDB.Effects

let maxBytes = 600
let decide = decideOperation maxBytes

let streamName = "stream1"

[<Fact>]
let ``Initial write`` () =
    let state = StreamState.Empty streamName
    let events = [| "event1"; "event2" |]

    let decision = decide state events
    let commands = matchDecision decision

    test <@ decision = InsertFirstDocument (events, streamName)@>
    test <@ commands = [|Start (events, streamName)|]@>


[<Fact>]
let ``Append`` () =
    let batchInfo = {
        StreamName=streamName 
        CurrentOffset=int64(1) 
        PreviousOffset= None 
        TotalSize=3 
        Etag="abcd"
    }
    let state = StreamState.Events batchInfo
    let newEvents = [| "event3"; "event4" |]

    let decision = decide state newEvents
    let commands = matchDecision decision

    test <@ decision = AppendToCurrent (newEvents, batchInfo) @>
    test <@ commands = [|Update (newEvents, batchInfo)|]@>


[<Fact>]
let ``Current is Full`` () =
    let batchInfo = {
        StreamName=streamName 
        CurrentOffset=int64(1) 
        PreviousOffset= None 
        TotalSize=maxBytes
        Etag="abcd"
    }
    let state = StreamState.Events batchInfo
    let newEvents = [| "eventBlob"; "eventBlob2" |]

    let decision = decide state newEvents
    let commands = matchDecision decision

    test <@ decision = Overflow ([||], newEvents, batchInfo) @>
    test <@ commands = [|Update ([||], batchInfo); Next (newEvents, batchInfo)|]@>

[<Fact>]
let ``Inserts Straddle two Documents`` () =
    let batchInfo = {
        StreamName=streamName 
        CurrentOffset=int64(1) 
        PreviousOffset= None 
        TotalSize= maxBytes - "eventBlob".Length - 3
        Etag="abcd"
    }
    let state = StreamState.Events batchInfo
    let event1, event2 = "eventBlob", "eventBlob2"
    let newEvents = [| event1; event2 |]

    let decision = decide state newEvents
    let commands = matchDecision decision

    test <@ decision = Overflow ([| event1 |], [| event2 |], batchInfo) @>
    test <@ commands = [|Update ([| event1 |], batchInfo); Next ([|event2|], batchInfo)|]@>



[<Fact>]
let ``Insert empty events`` () = 
    let batchInfo = {
        StreamName=streamName 
        CurrentOffset=int64(1) 
        PreviousOffset= None 
        TotalSize= maxBytes - "eventBlob".Length - 3
        Etag="abcd"
    }
    let state = StreamState.Events batchInfo
    let decision = decide state [||]
    let commands = matchDecision decision
    test <@ decision = Decision.NoOp @>
    test <@ commands = [|Command.Idle|] @>
    