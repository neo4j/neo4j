# Neo4j Tooling - Procedure|User Function Compiler

This is a annotation processor that will verify your stored procedures
at compile time.

While most of the basic checks can be performed, you still need
some unit tests to verify some runtime behaviours.


# What does it do?

Once the stored procedure compiler is added into your project classpath (see Maven/Gradle
instructions below), it will trigger compilation failures if any of the following requirements
is not met:

 - `@Context` fields must be `public` and non-`final`
 - all other fields must be `static`
 - `Map` record fields/procedure parameters must define their key type as `String`
 - `@Procedure`|`@UserFunction` class must define a public constructor with no arguments
 - `@Procedure` method must return a Stream
 - `@Procedure`|`@UserFunction` parameter and record types must be supported
 - `@Procedure`|`@UserFunction` parameters must be annotated with `@Name`
 - `@UserFunction` cannot be defined in the root namespace
 - all visited `@Procedure`|`@UserFunction` names must be unique*

*A deployed Neo4j instance can aggregate stored procedures from different JARs.
Inter-JAR naming conflict cannot be detected by an annotation processor.
By definition, it can only inspect one compilation unit at a time.

