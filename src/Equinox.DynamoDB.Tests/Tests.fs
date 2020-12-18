module Tests

open Xunit
open Swensen.Unquote
open Equinox.DynamoDB
open Equinox.DynamoDB.Events

let maxBytes = 600
let decide = decideOperation maxBytes

[<Fact>]
let ``Initial write`` () =
    let state = StreamState.Empty
    let events = [| "event1"; "event2" |]
    let result = decide state events
    test <@ result = Decision.InsertFirstDocument events @>


[<Fact>]
let ``Append`` () =
    let state = StreamState.Events (0, "abcd")
    let newEvents = [| "event3"; "event4" |]
    let result = decide state newEvents
    test <@ result = Decision.AppendToCurrent (newEvents, "abcd") @>


[<Fact>]
let ``Current is Full`` () =
    let state = StreamState.Events (maxBytes, "abcd")
    let newEvents = [| "eventBlob"; "eventBlob2" |]
    let result = decide state newEvents
    test <@ result = Decision.Overflow ([||], newEvents, "abcd") @>


[<Fact>]
let ``Inserts Straddle two Documents`` () =
    let state = StreamState.Events (maxBytes - "eventBlob".Length - 3, "abcd")
    let event1, event2 = "eventBlob", "eventBlob2"
    let newEvents = [| event1; event2 |]
    let result = decide state newEvents
    test <@ result = Decision.Overflow ([| event1 |], [| event2 |], "abcd") @>



[<Fact>]
let ``Insert empty events`` () = 
    let state = StreamState.Events (maxBytes, "abcd")
    let result = decide state [||]
    test <@ result = Decision.NoOp @>

[<Fact>]
let ``Insert into the DB`` () = 
    let state = StreamState.Events (maxBytes, "abcd")
    let result = decide state [||]
    test <@ result = Decision.NoOp @>
