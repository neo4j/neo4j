# Neo4j Data Protocol

This project contains a high-performance network protocol for Neo4j.
Tentatively called "Neo4j Data Protocol", NDP.

Architectural overview:

       [Worker Thread]

          Transport-[:USES]->(Messaging)-[:USES]->(Packstream)
             ^
          Runtime
             ^
            GDS


## Domain logic components

### Transport

Transports are responsible for moving messages from clients and into the runtime. They define the physical protocol.
Specifically:

- Physical message format & associated versioning
- Negotiating new sessions with clients, including authentication and versioning
- Ordered exactly-once delivery of messages with respect to a Session
- Connection loss tracking and associated resource cleanup
- Transport services like encryption and compression

### Runtime

In contrast to the transport which handles physical messages, this contains the coordinating heart of this protocol,
defining the runtime semantics of incoming messages and responses.

The main component in the runtime module is the Session State Machine, SSM. This defines the behavior of active
client sessions, what messages have what effects and when.

Mostly though, this is just a fancy box that delegates work to cypher and transaction management.

Clearly, this module is poorly named as it will cause confusion with the cypher runtime. Alternative name suggestions
welcome!

### Infrastructure components

These components contain code that defines infrastructure services that the main components require,
they are kept separately either because (like in the transport case) many components may want to use the same
services, as well as to keep infrastructure separate from the domain-level code.

#### Packstream

This is a serialization library, it takes java values and converts them into bytes. It supports ints, floats,
strings and booleans as well as lists, maps and structures.

#### Messaging

These are versioned implementations of how to convert the semantic Neo4j messages defined by the runtime into data
that can be transported over networks. It is separate from the transport modules because this can be shared across
multiple transports, and because we expect a transport could handle multiple versions of messaging formats for
backwards compatibility.

#### Streams (../)

This defines the 'stream' data structure that this protocol uses as its main mechanism of data transport. It is
expected that this module will be made obsolete by the new cypher runtime work. It is in a module separate from the
protocol runtime module, because both the runtime and the user extension modules (user extension module not part of
this version of the code yet) depend on it.