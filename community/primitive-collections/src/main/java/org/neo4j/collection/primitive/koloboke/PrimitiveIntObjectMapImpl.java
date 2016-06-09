/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.collection.primitive.koloboke;

import com.koloboke.collect.map.IntObjCursor;
import com.koloboke.compile.ConcurrentModificationUnchecked;
import com.koloboke.compile.KolobokeMap;

import java.util.Objects;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveIntVisitor;
import org.neo4j.collection.primitive.base.Hashing;

@SuppressWarnings( "ALL" )
@KolobokeMap
@ConcurrentModificationUnchecked
public abstract class PrimitiveIntObjectMapImpl<VALUE> implements PrimitiveIntObjectMap<VALUE>
{
    public static <VALUE> PrimitiveIntObjectMap<VALUE> withExceptedSize( int size )
    {
        return (PrimitiveIntObjectMap<VALUE>) new KolobokePrimitiveIntObjectMapImpl( size );
    }

    public abstract IntObjCursor<VALUE> cursor();

    @Override
    public final <E extends Exception> void visitEntries( PrimitiveIntObjectVisitor<VALUE, E> visitor ) throws E
    {
        IntObjCursor<VALUE> cursor = cursor();
        while ( cursor.moveNext() && !visitor.visited( cursor.key(), (VALUE) cursor.value() ) );
    }

    @Override
    public final <E extends Exception> void visitKeys( PrimitiveIntVisitor<E> visitor ) throws E
    {
        IntObjCursor<VALUE> cursor = cursor();
        while ( cursor.moveNext() && !visitor.visited( cursor.key() ) );
    }

    @Override
    public final PrimitiveIntIterator iterator()
    {
        final IntObjCursor<VALUE> cursor = cursor();
        return new PrimitiveIntIterator()
        {
            @Override
            public boolean hasNext()
            {
                return cursor.moveNext();
            }

            @Override
            public int next()
            {
                return cursor.key();
            }
        };
    }

    @Override
    public final void close()
    {
    }

    public final long defaultValue()
    {
        return -1;
    }

    @Override
    public final boolean equals( Object obj )
    {
        if ( obj instanceof PrimitiveIntObjectMap )
        {
            PrimitiveIntObjectMap map = (PrimitiveIntObjectMap) obj;
            if ( map.size() != size() )
            {
                return false;
            }
            IntObjCursor<VALUE> cursor = cursor();
            while ( cursor.moveNext() )
            {
                Object value = map.get( cursor.key() );
                if ( !Objects.equals( value, cursor.value() ) )
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public final int hashCode()
    {
        int hash = 1337;
        IntObjCursor<VALUE> cursor = cursor();
        while ( cursor.moveNext() )
        {
            hash += Hashing.xorShift( cursor.key() );
            hash += Objects.hashCode( cursor.value() );
        }
        return hash;
    }
}
