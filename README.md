Work-in-progress Kyoto Tycoon client for Haskell
===========

Right now, the code contains a base of code for connecting to Kyoto Tycoon
servers and running get/set operations. It uses the TSV-RPC interface, and
a custom HTTP client designed for maximum speed, for those times when you want
to be very efficient indeed. And it seems to work just fine.

The problem is, it's not complete (though it's mostly ready to go), it's not
properly documented, and there are so many optional arguments that the Haskell
type signatures for the API are getting unwieldy. And I'm short on time, and
there are already Haskell bindings for the almost feature equivalent Tokyo
Tyrant, which I've also contributed to:
[haskell-tyrant](https://github.com/PeterScott/haskell-tyrant/).

If you'd like to fork this and make it useful, I think that could be a fun project.
