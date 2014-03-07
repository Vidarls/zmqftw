// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open fszmq
open fszmq.Context
open fszmq.Socket
open fszmq.Polling

[<Literal>] 
let LRU_READY = "READY"

let decode = System.Text.Encoding.UTF8.GetString
let encode = string >> System.Text.Encoding.UTF8.GetBytes

let createClient context =
    let client = req context
    "tcp://127.0.0.1:5555" |> connect client
    client

let kill socket =
    (socket :> System.IDisposable).Dispose ()

let mutable gotReply = false

[<EntryPoint>]
let main argv = 
    printfn "Starting"
    use ctx = new Context()
    
    let handler seqNumber socket =
        let msg = socket |> recv |> decode |> System.Int32.Parse
        if msg = seqNumber then
            printfn "Server replied OK %A" msg
            gotReply <- true
        else
            printfn "Server replied shite %A" msg
    
    let rec run seqNumber expectingReply retriesLeft socket =
        poll 2500L [|Poll(ZMQ.POLLIN,socket,fun s -> handler seqNumber s)|] |> ignore
        match (expectingReply, retriesLeft, gotReply) with
        | false,_,_ -> printfn "Sending %A" seqNumber
                       gotReply <- false
                       seqNumber.ToString() |> encode |> send socket
                       run seqNumber true 3 socket
        | true,_,true -> run (seqNumber + 1) false 3 socket
        | true,0,false -> printfn "Giving up..."
                          kill socket
        | true,_,false -> printfn "No response from server retrying" 
                          kill socket
                          run seqNumber true (retriesLeft - 1) (createClient ctx)
    
    run 0 false 3 (createClient ctx)
     
    0 // return an integer exit code
