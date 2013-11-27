Today (2007-11-19) AGPL was released so we can finally release the source code.

To start hacking, import src/main/java into your favorite IDE and add the 
geronimo-jta jar file to your classpath.

To get started here is a short description of some of the important packages:

o kernel & kernel.impl.core, implementation of the Neo4j API
o kernel.impl.nioneo, native store implementation
o kernel.impl.transaction, TM + RWLocks implementation
o kernel.impl.transaction.xaframework, make non XA resources XA enabled

