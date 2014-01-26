package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import static java.lang.String.format;

public class RegisterSignature
{
    private final int objectRegisters;
    private final int entityRegisters;

    public RegisterSignature()
    {
        this( 0, 0 );
    }

    public RegisterSignature( int objectRegisters, int entityRegisters )
    {
        this.objectRegisters = ensurePositiveOrNull( "Number of object registers", objectRegisters );
        this.entityRegisters = ensurePositiveOrNull( "Number of entity registers", entityRegisters );
    }

    public int objectRegisters()
    {
        return objectRegisters;
    }

    public int entityRegisters()
    {
        return entityRegisters;
    }

    public RegisterSignature withObjectRegisters( int newObjectRegisters )
    {
        return new RegisterSignature( newObjectRegisters, entityRegisters );
    }

    public RegisterSignature withEntityRegisters( int newEntityRegisters )
    {
        return new RegisterSignature( objectRegisters, newEntityRegisters );
    }

    private int ensurePositiveOrNull( String what, int number )
    {
        if ( number < 0 )
        {
            throw new IllegalArgumentException( format( "%s expected to be >= 0, but is: %d", what, number ) );
        }
        return number;
    }
}
