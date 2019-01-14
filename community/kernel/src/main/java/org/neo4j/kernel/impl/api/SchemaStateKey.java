/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.api;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Recommended way to create keys for {@link SchemaState}, to guarantee control over equality uniqueness.
 */
public class SchemaStateKey
{
    private static AtomicLong keyId = new AtomicLong();
    public static SchemaStateKey newKey()
    {
        return new SchemaStateKey( keyId.getAndIncrement() );
    }

    public final long id;

    private SchemaStateKey( long id )
    {
        this.id = id;
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
        SchemaStateKey that = (SchemaStateKey) o;
        return id == that.id;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode( id );
    }

    @Override
    public String toString()
    {
        return "SchemaStateKey(" + id + ")";
    }
}
