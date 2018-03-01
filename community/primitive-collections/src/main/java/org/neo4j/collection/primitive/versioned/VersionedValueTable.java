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
package org.neo4j.collection.primitive.versioned;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.hopscotch.LongKeyTable;

import static java.util.Arrays.fill;

class VersionedValueTable<V> extends LongKeyTable<VersionedEntry<V>>
{
    private final VersionedCollection versionedCollection;
    private final VersionedEntry<V> transportRecord;
    private VersionedEntry<V>[] entries;

    VersionedValueTable( VersionedCollection versionedCollection )
    {
        this( Primitive.DEFAULT_HEAP_CAPACITY, versionedCollection );
    }

    private VersionedValueTable( int capacity, VersionedCollection versionedCollection )
    {
        super( capacity, null );
        this.versionedCollection = versionedCollection;
        this.transportRecord = new VersionedEntry<>( versionedCollection );
    }

    @Override
    public VersionedEntry<V> value( int index )
    {
        return entries[index];
    }

    @Override
    public void put( int index, long key, VersionedEntry<V> value )
    {
        super.put( index, key, value );
        entries[index] = value;
    }

    @Override
    public VersionedEntry<V> remove( int index )
    {
        final VersionedEntry<V> value = value( index );
        transportRecord.clear();
        if ( value != null )
        {
            transportRecord.setCurrentValue( value.getCurrentValue(), value.getCurrentVersion() );
            value.setCurrentValue( null, versionedCollection.currentVersion() );
            if ( value.getStableValue() == null )
            {
                purgeEntry( index );
            }
        }
        return transportRecord;
    }

    @Override
    public VersionedEntry<V> putValue( int index, VersionedEntry<V> value )
    {
        VersionedEntry<V> entry = entries[index];
        entry.setCurrentValue( value.getCurrentValue(), value.getCurrentVersion() );
        return entry;
    }

    @Override
    public long move( int fromIndex, int toIndex )
    {
        entries[toIndex] = entries[fromIndex];
        entries[fromIndex] = null;
        return super.move( fromIndex, toIndex );
    }

    @Override
    protected VersionedValueTable<V> newInstance( int newCapacity )
    {
        return new VersionedValueTable<V>( newCapacity, versionedCollection );
    }

    @Override
    protected void clearTable()
    {
        super.clearTable();
        fill( entries, null );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected void initializeTable()
    {
        super.initializeTable();
        entries = (VersionedEntry<V>[]) new VersionedEntry[capacity];
    }

    void purgeEntry( int index )
    {
        super.remove( index );
    }
}
