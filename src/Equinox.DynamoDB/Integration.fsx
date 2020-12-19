#r "bin/Debug/net5.0/Equinox.DynamoDB.dll"
#r "nuget: AWSSDK.DynamoDBv2 ,Version=3.5.4.8"
#r "nuget: FSharp.AWS.DynamoDB ,Version=0.8.1-beta"

// open System
// open FSharp.AWS
// open FSharp.AWS.DynamoDB
open Amazon.DynamoDBv2
open Equinox.DynamoDB
open Equinox.DynamoDB.Events
open Equinox.DynamoDB.Effects


let config = AmazonDynamoDBConfig(ServiceURL="http://localhost:8000")

// printfn "Config %A" config.ServiceURL
// printfn "Config %A" config.
// printfn "Config %A" config.ServiceURL
let client = new AmazonDynamoDBClient(config)
let tableName = "equinox"
let table = createtableContext client tableName

let maxBytes = 600
let decide = decideOperation maxBytes

let batchInfoFullStream streamName offset= {
    StreamName=streamName 
    CurrentOffset=int64(offset)
    PreviousOffset= None 
    TotalSize=maxBytes
    Etag="2be80c56-1929-432a-8ddf-c2cc6d353cfa"
}

let batchInfoAlmostFullStream streamName offset remaining= {
    StreamName=streamName 
    CurrentOffset=int64(offset)
    PreviousOffset= Some (int64(offset - 1)) 
    TotalSize=maxBytes - remaining
    Etag="6170700e-37e1-4212-8125-13afd96c514a"
}
// let decision = decide (StreamState.Empty "Stream1") ([| "event1"; "event2" |])

// let decision = decide (StreamState.Events (batchInfoFullStream "Stream1" 0)) ([|"event3"; "event4"|])
let decision = decide (StreamState.Events (batchInfoAlmostFullStream "Stream1" 1 6)) ([|"event5"; "event6"|])

Array.map (fun c -> (applyCommand table c)) (matchDecision decision)