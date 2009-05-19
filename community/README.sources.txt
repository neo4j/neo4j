Today (2007-11-19) AGPL was released so we can finally release the source code.

To start hacking, import src/java into your favorite IDE and add the 
jta-1_1.jar to your classpath.

To get started here is a short description of some of the important packages:

o impl.core, implementation of the Neo4j API
o impl.nioneo, native store implementation
o impl.transaction, TM + RWLocks implementation
o impl.transaction.xaframework, XA framework to make non XA resources XA enabled

