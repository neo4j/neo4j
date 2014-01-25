package org.neo4j.cypher.internal.compiler.v2_1.runtime;

public interface Register
{
    Registers registers();

    Object getObject();
    void setObject( Object value );

    void copyFrom( Registers registers );
    void copyTo( Registers registers );
}
