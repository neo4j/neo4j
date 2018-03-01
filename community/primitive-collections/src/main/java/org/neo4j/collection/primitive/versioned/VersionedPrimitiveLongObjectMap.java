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

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.function.IntSupplier;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.collection.primitive.hopscotch.AbstractLongHopScotchCollection;
import org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm;
import org.neo4j.collection.primitive.hopscotch.MapValueIterator;
import org.neo4j.collection.primitive.hopscotch.Table;
import org.neo4j.collection.primitive.hopscotch.TableKeyIterator;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.DEFAULT_HASHING;
import static org.neo4j.collection.primitive.hopscotch.HopScotchHashingAlgorithm.NO_MONITOR;

public class VersionedPrimitiveLongObjectMap<V> implements HopScotchHashingAlgorithm.ResizeMonitor<VersionedEntry<V>>, VersionedCollection
{
    private final StableVersionedLongObjectMapView<V> stableView;
    private final CurrentVersionedLongObjectMapView<V> currentView;
    private VersionedValueTable<V> versionedMap;
    private int stableVersion;
    private int currentVersion;

    public VersionedPrimitiveLongObjectMap()
    {
        init();
        this.stableView = new StableVersionedLongObjectMapView<>( this );
        this.currentView = new CurrentVersionedLongObjectMapView<>( this );
    }

    @Override
    public void markStable()
    {
        stableVersion = currentVersion;
        currentVersion++;
    }

    public PrimitiveLongObjectMap<V> stableView()
    {
        return stableView;
    }

    public PrimitiveLongObjectMap<V> currentView()
    {
        return currentView;
    }

    public void clear()
    {
        init();
    }

    private void init()
    {
        stableVersion = 0;
        currentVersion = 0;
        this.versionedMap = new VersionedValueTable<>( this );
        markStable();
    }

    @Override
    public void tableGrew( Table<VersionedEntry<V>> newTable )
    {
        versionedMap = (VersionedValueTable<V>) newTable;
    }

    @Override
    public Table<VersionedEntry<V>> getLastTable()
    {
        return versionedMap;
    }

    @Override
    public int currentVersion()
    {
        return currentVersion;
    }

    @Override
    public int stableVersion()
    {
        return stableVersion;
    }

    private static class StableVersionedLongObjectMapView<V> extends AbstractLongHopScotchCollection<V> implements PrimitiveLongObjectMap<V>
    {
        private final VersionedPrimitiveLongObjectMap<V> longVersionedMap;

        StableVersionedLongObjectMapView( VersionedPrimitiveLongObjectMap<V> longVersionedMap )
        {
            super( (Table<V>) longVersionedMap.versionedMap );
            this.longVersionedMap = longVersionedMap;
        }

        @Override
        public V put( long key, V v )
        {
            throw new UnsupportedOperationException( "Modification of stable revision are not supported." );
        }

        @Override
        public boolean containsKey( long key )
        {
            VersionedEntry<V> entry = HopScotchHashingAlgorithm.get( longVersionedMap.versionedMap, NO_MONITOR, DEFAULT_HASHING, key );
            return entry != null && entry.getStableValue() != null;
        }

        @Override
        public V get( long key )
        {
            VersionedEntry<V> entry =
                    HopScotchHashingAlgorithm.get( longVersionedMap.versionedMap, NO_MONITOR, DEFAULT_HASHING, key );
            return entry != null ? entry.getStableValue() : null;
        }

        @Override
        public V remove( long key )
        {
            throw new UnsupportedOperationException( "Modification of stable revision are not supported." );
        }

        @Override
        public <E extends Exception> void visitEntries( PrimitiveLongObjectVisitor<V, E> visitor ) throws E
        {
            throw new UnsupportedOperationException( "Entries visiting not supported." );
        }

        @Override
        public Iterable<V> values()
        {
            return new ValueIterable( this, longVersionedMap::stableVersion );
        }

        @Override
        public <E extends Exception> void visitKeys( PrimitiveLongVisitor<E> visitor ) throws E
        {
            throw new UnsupportedOperationException( "Keys visiting not supported." );
        }

        @Override
        public boolean isEmpty()
        {
            return size() == 0;
        }

        @Override
        public void clear()
        {
            throw new UnsupportedOperationException( "Modification of stable revision are not supported." );
        }

        @Override
        public int size()
        {
            int size = 0;
            PrimitiveLongIterator iterator = this.iterator();
            while ( iterator.hasNext() )
            {
                iterator.next();
                size++;
            }
            return size;
        }

        @Override
        public void close()
        {
            // nothing to close
        }

        @Override
        public boolean equals( Object other )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrimitiveLongIterator iterator()
        {
            return new VersionedTableKeyIterator<>( longVersionedMap.versionedMap, this, longVersionedMap::stableVersion );
        }
    }

    private static class CurrentVersionedLongObjectMapView<V> extends AbstractLongHopScotchCollection<V> implements PrimitiveLongObjectMap<V>
    {
        private final VersionedPrimitiveLongObjectMap<V> longVersionedMap;

        private CurrentVersionedLongObjectMapView( VersionedPrimitiveLongObjectMap<V> longVersionedMap )
        {
            super( (Table<V>) longVersionedMap.versionedMap );
            this.longVersionedMap = longVersionedMap;
        }

        @Override
        public V put( long key, V v )
        {
            requireNonNull( v, "Can't store null values" );
            VersionedEntry<V> entry = HopScotchHashingAlgorithm
                    .put( longVersionedMap.versionedMap, NO_MONITOR, DEFAULT_HASHING, key,
                            new VersionedEntry<>( longVersionedMap.currentVersion, v, longVersionedMap ),
                            longVersionedMap );
            return entry != null ? entry.getCurrentValue() : null;
        }

        @Override
        public boolean containsKey( long key )
        {
            final VersionedEntry<V> entry = HopScotchHashingAlgorithm.get( longVersionedMap.versionedMap, NO_MONITOR, DEFAULT_HASHING, key );
            return entry != null && entry.getCurrentValue() != null;
        }

        @Override
        public V get( long key )
        {
            VersionedEntry<V> entry =
                    HopScotchHashingAlgorithm.get( longVersionedMap.versionedMap, NO_MONITOR, DEFAULT_HASHING, key );
            return entry != null ? entry.getCurrentValue() : null;
        }

        @Override
        public V remove( long key )
        {
            VersionedEntry<V> entry =
                    HopScotchHashingAlgorithm.remove( longVersionedMap.versionedMap, NO_MONITOR, DEFAULT_HASHING, key );
            return entry != null ? entry.getCurrentValue() : null;
        }

        @Override
        public <E extends Exception> void visitEntries( PrimitiveLongObjectVisitor<V, E> visitor ) throws E
        {
            throw new UnsupportedOperationException( "Entries visiting not supported." );
        }

        @Override
        public Iterable<V> values()
        {
            return new ValueIterable( this, longVersionedMap::currentVersion );
        }

        @Override
        public <E extends Exception> void visitKeys( PrimitiveLongVisitor<E> visitor ) throws E
        {
            throw new UnsupportedOperationException( "Keys visiting not supported." );
        }

        @Override
        public boolean isEmpty()
        {
            return size() == 0;
        }

        @Override
        public void clear()
        {
            // nothing to clear
        }

        @Override
        public int size()
        {
            int size = 0;
            PrimitiveLongIterator iterator = this.iterator();
            while ( iterator.hasNext() )
            {
                iterator.next();
                size++;
            }
            return size;
        }

        @Override
        public void close()
        {
            // nothing to close
        }

        @Override
        public boolean equals( Object other )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PrimitiveLongIterator iterator()
        {
            return new VersionedTableKeyIterator<>( longVersionedMap.versionedMap, this, longVersionedMap::currentVersion );
        }
    }

    private static class VersionedTableKeyIterator<V> extends TableKeyIterator<V>
    {
        private final VersionedValueTable<V> table;
        private final PrimitiveLongObjectMap<V> map;
        private final IntSupplier versionSupplier;
        private final int startVersion;

        VersionedTableKeyIterator( VersionedValueTable<V> versionedMap, PrimitiveLongObjectMap<V> map, IntSupplier versionSupplier )
        {
            super( (Table<V>) versionedMap );
            this.table = versionedMap;
            this.map = map;
            this.versionSupplier = versionSupplier;
            this.startVersion = versionSupplier.getAsInt();
        }

        @Override
        protected boolean next( long nextItem )
        {
            final int currentVersion = versionSupplier.getAsInt();
            if ( startVersion != currentVersion )
            {
                throw new ConcurrentModificationException( format( "Map version changed: old=%d, current=%d", startVersion, currentVersion ) );
            }
            return super.next( nextItem );
        }

        @Override
        protected boolean isVisible( int index, long key )
        {
            if ( !super.isVisible( index, key ) )
            {
                return false;
            }
            if ( map.containsKey( key ) )
            {
                return true;
            }
            if ( table.value( index ).isOrphan() )
            {
                table.purgeEntry( index );
            }
            return false;
        }
    }

    private static class ValueIterable<V> implements Iterable<V>
    {
        private final PrimitiveLongObjectMap<V> map;
        private final IntSupplier versionSupplier;
        private final int startVersion;

        ValueIterable( PrimitiveLongObjectMap<V> map, IntSupplier versionSupplier )
        {
            this.map = map;
            this.versionSupplier = versionSupplier;
            this.startVersion = versionSupplier.getAsInt();
        }

        @Override
        public Iterator<V> iterator()
        {
            return new MapValueIterator<V>( map )
            {
                @Override
                public V next()
                {
                    final int currentVersion = versionSupplier.getAsInt();
                    if ( startVersion != currentVersion )
                    {
                        throw new ConcurrentModificationException( format( "Map version changed: old=%d, current=%d", startVersion, currentVersion ) );
                    }
                    return super.next();
                }
            };
        }
    }
}
