/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.collection.primitive;

import java.util.Objects;
import java.util.function.LongFunction;

public interface PrimitiveLongObjectMap<VALUE> extends PrimitiveLongCollection
{
    VALUE put( long key, VALUE value );

    boolean containsKey( long key );

    VALUE get( long key );

    VALUE remove( long key );

    /**
     * Visit the entries of this map, until all have been visited or the visitor returns 'true'.
     */
    <E extends Exception> void visitEntries( PrimitiveLongObjectVisitor<VALUE,E> visitor ) throws E;

    /**
     * {@link Iterable} with all map values
     *
     * @return iterable with all map values
     */
    Iterable<VALUE> values();

    default VALUE computeIfAbsent( long key, LongFunction<VALUE> function )
    {
        Objects.requireNonNull( function );
        VALUE value = get( key );
        if ( value == null )
        {
            value = function.apply( key );
            put( key, value );
        }
        return value;
    }
}
