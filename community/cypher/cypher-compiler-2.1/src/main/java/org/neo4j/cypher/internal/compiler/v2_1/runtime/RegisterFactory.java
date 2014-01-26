package org.neo4j.cypher.internal.compiler.v2_1.runtime;

public interface RegisterFactory
{
    Registers createRegisters( RegisterSignature signature );
}
