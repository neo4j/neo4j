/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
