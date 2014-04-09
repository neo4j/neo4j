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
package org.neo4j.util.primitive.collection.hopscotch;

import org.neo4j.util.primitive.collection.PrimitiveLongIterator;
import org.neo4j.util.primitive.collection.PrimitiveLongSet;
import org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.HashFunction;
import org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.Monitor;

import static org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.DEFAULT_H;
import static org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;
import static org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.NO_MONITOR;

public class VersionedPrimitiveLongHashSet extends AbstractHopScotchCollection<Object> implements PrimitiveLongSet
{
    private static final Object THE_VALUE = new Object();

    private final HashFunction hashFunction; // TODO needed as a member (or just stick with a default always)?
    private final Monitor monitor;

    public VersionedPrimitiveLongHashSet( int h, HashFunction hashFunction, Monitor monitor )
    {
        super( new VersionedLongKeyTable<>( h, THE_VALUE ) );
        this.hashFunction = hashFunction;
        this.monitor = monitor;
    }

    public VersionedPrimitiveLongHashSet()
    {
        this( DEFAULT_H, DEFAULT_HASHING, NO_MONITOR );
    }

    @Override
    public boolean add( long value )
    {
        return HopScotchHashingAlgorithm.put( table, monitor, hashFunction, value, THE_VALUE, this ) == null;
    }

    @Override
    public boolean addAll( PrimitiveLongIterator values )
    {
        boolean changed = false;
        while ( values.hasNext() )
        {
            changed |= HopScotchHashingAlgorithm.put( table, monitor, hashFunction, values.next(),
                    THE_VALUE, this ) == null;
        }
        return changed;
    }

    @Override
    public boolean contains( long value )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, hashFunction, value ) == THE_VALUE;
    }

    @Override
    public boolean remove( long value )
    {
        return HopScotchHashingAlgorithm.remove( table, monitor, hashFunction, value ) == THE_VALUE;
    }

    @Override
    public PrimitiveLongIterator iterator()
    {
        return new VersionedTableIterator<>( table, this );
    }
}
