#r "bin/Debug/net5.0/Equinox.DynamoDB.dll"
#r "nuget: AWSSDK.DynamoDBv2 ,Version=3.5.4.8"
#r "nuget: FSharp.AWS.DynamoDB ,Version=0.8.1-beta"

open System
open FSharp.AWS.DynamoDB
open Amazon.DynamoDBv2
open Equinox.DynamoDB
open Equinox.DynamoDB.Events
open Equinox.DynamoDB.Effects

let client = new AmazonDynamoDBClient()
let tableName = "equinox"
let table = createtableContext client tableName

let maxBytes = 600
let decide = decideOperation maxBytes
let state = StreamState.Empty
let events = [| "event1"; "event2" |]
let decision = decide state events

generateDynamoEffect table "stream1" decision