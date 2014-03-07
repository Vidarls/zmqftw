// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.
open fszmq
open fszmq.Socket
open fszmq.Context
open fszmq.Polling

[<Literal>] 
let PPP_READY = "READY"
[<Literal>]
let PPP_HEARTBEAT = "HEARTBEAT"
[<Literal>]
let HEARTBEAT_LIVENESS = 3.0
[<Literal>]
let HEARTBEAT_INTERVAL = 1000.0

let srandom ()  = System.Random(System.DateTime.Now.Millisecond)
let private rnd = srandom() // for use by ``s_setID``
let encode = string >> System.Text.Encoding.UTF8.GetBytes
let decode = System.Text.Encoding.UTF8.GetString
let identity () = sprintf "%04X-%04X" 
                         (rnd.Next(0,0x10000)) 
                         (rnd.Next(0,0x10000))
                 |> encode
let s_setID socket id = 
  (ZMQ.IDENTITY, id) |> Socket.setOption socket

let unwrap msg =
    let head = msg |> Seq.head
    if (msg |> Seq.skip 1 |> Seq.head) = [||] then 
        (head, (msg |> Seq.skip 2))
    else
        (head, (msg |> Seq.skip 1)) 

let getData msg =
    msg |> Seq.head

let createConnectedWorker context =
    let worker = deal context
    let id = (identity ())
    s_setID worker id
    "tcp://127.0.0.1:5556" |> connect worker
    printfn "Worker %A ready!" id
    PPP_READY |> encode |> send worker
    worker 

let kill socket =
    (socket :> System.IDisposable).Dispose ()

let mutable interval = HEARTBEAT_INTERVAL
let mutable liveness = HEARTBEAT_LIVENESS
let mutable cycles = 0

[<EntryPoint>]
let main argv = 
    printfn "Starting"
    use ctx = new Context()
    
    let messageHandler socket =
        let (id,msg) = socket |> recvAll |> unwrap
        printfn "Received id: %A msg %A" id msg
        interval <- HEARTBEAT_INTERVAL
        liveness <- HEARTBEAT_LIVENESS
        match msg |> getData |> decode with
        | PPP_HEARTBEAT -> printfn "Heartbeat"                  
        | _             -> printfn "Replying %A" msg
                           msg |> sendAll socket
    
    let rec run worker timeForHeartbeat =
        let poller = [|Poll(ZMQ.POLLIN,worker, fun s -> messageHandler s)|]
        poll (HEARTBEAT_INTERVAL |> System.Convert.ToInt64) poller |> ignore
        let isItTime = (System.DateTime.UtcNow > timeForHeartbeat)
        liveness <- (liveness - 1.0)
        match (liveness, isItTime) with 
        | (x,_) when x <= 0.0 -> printfn "HB failure"
                                 printfn "Reconnecting..."
                                 (System.Threading.Thread.Sleep(interval |> System.Convert.ToInt32)) 
                                 if interval < 32000.0 then interval <- interval * 2.0
                                 liveness <- HEARTBEAT_LIVENESS
                                 kill worker
                                 run (createConnectedWorker ctx) timeForHeartbeat
        | (_,true)            -> PPP_HEARTBEAT |> encode |> send worker 
                                 printfn "Time for heartbeat"
                                 run worker (System.DateTime.UtcNow.AddMilliseconds(HEARTBEAT_INTERVAL))
        | (_,_)               -> run worker timeForHeartbeat
   



    run (createConnectedWorker ctx) (System.DateTime.UtcNow.AddMilliseconds(HEARTBEAT_INTERVAL))

    0 // return an integer exit code
