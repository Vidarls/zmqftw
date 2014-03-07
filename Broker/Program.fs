// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.

open fszmq
open fszmq.Context
open fszmq.Socket
open fszmq.Polling

[<Literal>] 
let PPP_READY = "READY"
[<Literal>]
let PPP_HEARTBEAT = "HEARTBEAT"
[<Literal>]
let HEARTBEAT_LIVENESS = 3.0
[<Literal>]
let HEARTBEAT_INTERVAL = 1000.0

let decode = System.Text.Encoding.UTF8.GetString
let encode = string >> System.Text.Encoding.UTF8.GetBytes

let unwrap msg =
    let head = msg |> Seq.head
    if (msg |> Seq.skip 1 |> Seq.head) = [||] then 
        (head, (msg |> Seq.skip 2))
    else
        (head, (msg |> Seq.skip 1)) 

let wrap (msg:seq<byte[]>) (newPart:seq<byte[]>) = 
    let head = [|[||]|] |> Seq.append newPart
    let complete = msg |> Seq.append head
    complete

let getData msg =
    msg |> Seq.head

type ExpiresAt = System.DateTime

let addOrRefresh (workers:Map<byte[],ExpiresAt>) id =
    let workers = if workers.ContainsKey id then workers.Remove id else workers
    workers.Add(id, ExpiresAt.UtcNow.AddMilliseconds((HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS)))

let getNextWorker (workers:Map<byte[],ExpiresAt>) =
    let (id, _) = workers |> Map.toSeq |> Seq.head
    id

let workerIsBusy (workers:Map<byte[],ExpiresAt>) id =
    workers.Remove id

let mutable workers = Map.empty<byte[],ExpiresAt>        
    
let sendHeartbeat socket id =
    let heartBeat = PPP_HEARTBEAT |> encode
    let toSend = id |> wrap [|heartBeat|] 
    printfn "HB: %A" toSend
    toSend |> sendAll socket


[<EntryPoint>]
let main argv = 
    printfn "Starting" 
    use ctx = new Context()
    use frontend = route ctx
    use backend = route ctx

    "tcp://*:5555" |> bind frontend
    "tcp://*:5556" |> bind backend

    let backendHandler socket =
        let (id,msg) = socket |> recvAll |> unwrap
        workers <- id |> addOrRefresh workers
        match msg |> getData |> decode with
        | PPP_READY       -> printfn "WORKER %A ready" (id |> decode)                            
        | PPP_HEARTBEAT   -> printfn "HEARTBEAT FROM %s" (id |> decode)                            
        | _               -> printfn "Sending to FRONTEND %A" msg
                             msg |> sendAll frontend

    let frontendHandler socket =
        let msg = socket |> recvAll
        let worker = getNextWorker workers
        workers <- worker |> workerIsBusy workers
        let toSend = [|worker|] |> wrap msg 
        printfn "SENDING TO BACKEND %A" toSend
        toSend |> sendAll backend
    
    let getPollers () = 
        let backendPoller = Poll(ZMQ.POLLIN,backend, fun s -> backendHandler s)
        let frontendPoller = Poll(ZMQ.POLLIN, frontend, fun s-> frontendHandler s)
        match workers.Count with
        | 0 -> printfn "NO WORKERS"
               [|backendPoller|]
        | _ -> printfn "%i WORKERS" workers.Count
               [|backendPoller; frontendPoller|]
    
    let rec run heartbeatAt pollers =
        poll (HEARTBEAT_INTERVAL |> System.Convert.ToInt64) pollers |> ignore
        match ExpiresAt.UtcNow >= heartbeatAt with
        | true ->  printfn "Time for heartbeat"
                   workers |> Map.toSeq |> Seq.iter (fun (id, _) -> sendHeartbeat backend [|id|] )
                   run (ExpiresAt.UtcNow.AddMilliseconds(HEARTBEAT_INTERVAL)) (getPollers ())
        | false -> run heartbeatAt (getPollers ()) 

    run (ExpiresAt.UtcNow.AddMilliseconds(HEARTBEAT_INTERVAL)) (getPollers ())
     
    0 // return an integer exit code
