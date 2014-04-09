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
import org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.HashFunction;
import org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.Monitor;

import static org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.DEFAULT_H;
import static org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;
import static org.neo4j.util.primitive.collection.hopscotch.HopScotchHashingAlgorithm.NO_MONITOR;

public class PrimitiveLongObjectHashMap<VALUE> extends AbstractHopScotchCollection<VALUE>
        implements PrimitiveLongObjectMap<VALUE>
{
    private final HashFunction hashFunction;
    private final Monitor monitor;

    public PrimitiveLongObjectHashMap( int h, HashFunction hashFunction, Monitor monitor )
    {
        super( new LongKeyObjectValueTable<VALUE>( h ) );
        this.hashFunction = hashFunction;
        this.monitor = monitor;
    }

    public PrimitiveLongObjectHashMap( int h )
    {
        this( h, DEFAULT_HASHING, NO_MONITOR );
    }

    public PrimitiveLongObjectHashMap()
    {
        this( DEFAULT_H, DEFAULT_HASHING, NO_MONITOR );
    }

    @Override
    public VALUE put( long key, VALUE value )
    {
        return HopScotchHashingAlgorithm.put( table, monitor, hashFunction, key, value, this );
    }

    @Override
    public boolean containsKey( long key )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, hashFunction, key ) != null;
    }

    @Override
    public VALUE get( long key )
    {
        return HopScotchHashingAlgorithm.get( table, monitor, hashFunction, key );
    }

    @Override
    public VALUE remove( long key )
    {
        return HopScotchHashingAlgorithm.remove( table, monitor, hashFunction, key );
    }

    @Override
    public int size()
    {
        return table.size();
    }

    @Override
    public PrimitiveLongIterator keyIterator()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}
