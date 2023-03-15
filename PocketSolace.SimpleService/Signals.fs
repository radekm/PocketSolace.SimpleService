module PocketSolace.SimpleService.Signals

open System.Runtime.InteropServices

let private sync = obj()  // For locking.
let private registrations = ResizeArray()

/// Registers single handler for SIGHUP, SIGINT, SIGQUIT and SIGTERM.
/// Handlers registered by this function cannot be unregistered.
///
/// Can be called at most once.
let registerPermanentHandler (handler : PosixSignal -> unit) =
    let registerOne (signal : PosixSignal) =
        PosixSignalRegistration.Create(signal, fun context ->
            handler signal
            context.Cancel <- true)
    lock sync <| fun () ->
        if registrations.Count > 0 then
            failwith "Signal handler already registered"
        [ PosixSignal.SIGHUP
          PosixSignal.SIGINT
          PosixSignal.SIGQUIT
          PosixSignal.SIGTERM ]
        // We put registrations into `registrations` to ensure they are not
        // unregistered (which would happen if they're garbage collected and finalized).
        |> List.iter (fun signal -> registerOne signal |> registrations.Add)
