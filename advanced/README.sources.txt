Today (2007-11-19) AGPL was released so we can finally release the source code.
This is a pre release, we're not 1.0 yet and haven't had time to clean up the 
sources.  

To start hacking import src/java into your favorite IDE and add the 
jta-spec-1_0_1.jar to your classpath.

To run the unit tests make sure you have some version of junit in your 
classpath (3.x+ should do), import the src/test sources and run 
unit.neo.TestAll as a java application.

To get started here is a short description of some of the important packages:

o impl.core, implementation of the Neo API
o impl.event, event framework (used to decouple persistence layer)
o impl.nioneo, native store implementation
o impl.transaction, TM + RWLocks implementation
o impl.transaction.xaframework, XA framework to make non XA resources XA enabled

