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
package org.neo4j.collection.primitive.hopscotch;

import org.neo4j.collection.primitive.PrimitiveLongCollection;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;

public abstract class AbstractLongHopScotchCollection<VALUE> extends AbstractHopScotchCollection<VALUE>
        implements PrimitiveLongCollection
{
    public AbstractLongHopScotchCollection( Table<VALUE> table )
    {
        super( table );
    }

    @Override
    public PrimitiveLongIterator iterator()
    {
        return new TableKeyIterator<>( table, this );
    }

    @Override
    public <E extends Exception> void visitKeys( PrimitiveLongVisitor<E> visitor ) throws E
    {
        if ( table.isEmpty() )
        {
            return;
        }

        int capacity = table.capacity();
        long nullKey = table.nullKey();
        for ( int i = 0; i < capacity; i++ )
        {
            long key = table.key( i );
            if ( key != nullKey && visitor.visited( key ) )
            {
                return;
            }
        }
    }
}
