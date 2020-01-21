/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api.state;


import java.util.Objects;

import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.memory.HeapEstimator.sizeOf;

/**
 * The transaction state of a token that we want to create.
 *
 * We track the name, and whether or not the token is internal or public.
 */
class TokenState
{
    private static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( TokenState.class );
    public final String name;
    public final boolean internal;

    static TokenState createTokenState( String name, boolean internal, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( SHALLOW_SIZE + sizeOf( name ) );
        return new TokenState( name, internal );
    }

    private TokenState( String name, boolean internal )
    {
        this.name = name;
        this.internal = internal;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        TokenState that = (TokenState) o;
        return internal == that.internal && name.equals( that.name );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name, internal );
    }
}
