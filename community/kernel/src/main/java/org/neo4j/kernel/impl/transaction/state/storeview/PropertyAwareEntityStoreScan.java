/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Iterator;
import java.util.function.IntPredicate;
import java.util.function.LongFunction;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.api.index.EntityUpdates;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreIdIterator;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

public abstract class PropertyAwareEntityStoreScan<RECORD extends PrimitiveRecord, FAILURE extends Exception> implements StoreScan<FAILURE>
{
    private final RecordStore<RECORD> store;
    private volatile boolean continueScanning;
    private long count;
    private long totalCount;
    private final IntPredicate propertyKeyIdFilter;
    private final LongFunction<Lock> lockFunction;
    private final PropertyStore propertyStore;
    private final RECORD record;

    protected PropertyAwareEntityStoreScan( RecordStore<RECORD> store, PropertyStore propertyStore, IntPredicate propertyKeyIdFilter,
            LongFunction<Lock> lockFunction )
    {
        this.store = store;
        this.propertyStore = propertyStore;
        this.record = store.newRecord();
        this.totalCount = store.getHighId();
        this.propertyKeyIdFilter = propertyKeyIdFilter;
        this.lockFunction = lockFunction;
    }

    static boolean containsAnyEntityToken( int[] entityTokenFilter, long... entityTokens )
    {
        for ( long candidate : entityTokens )
        {
            if ( ArrayUtils.contains( entityTokenFilter, Math.toIntExact( candidate ) ) )
            {
                return true;
            }
        }
        return false;
    }

    boolean hasRelevantProperty( RECORD record, EntityUpdates.Builder updates )
    {
        boolean hasRelevantProperty = false;

        for ( PropertyBlock property : properties( record ) )
        {
            int propertyKeyId = property.getKeyIndexId();
            if ( propertyKeyIdFilter.test( propertyKeyId ) )
            {
                // This relationship has a property of interest to us
                Value value = valueOf( property );
                // No need to validate values before passing them to the updater since the index implementation
                // is allowed to fail in which ever way it wants to. The result of failure will be the same as
                // a failed validation, i.e. population FAILED.
                updates.added( propertyKeyId, value );
                hasRelevantProperty = true;
            }
        }
        return hasRelevantProperty;
    }

    @Override
    public void run() throws FAILURE
    {
        try ( PrimitiveLongResourceIterator entityIdIterator = getEntityIdIterator() )
        {
            continueScanning = true;
            while ( continueScanning && entityIdIterator.hasNext() )
            {
                long id = entityIdIterator.next();
                try ( Lock ignored = lockFunction.apply( id ) )
                {
                    count++;
                    if ( store.getRecord( id, this.record, FORCE ).inUse() )
                    {
                        process( this.record );
                    }
                }
            }
        }
    }

    protected abstract void process( RECORD record ) throws FAILURE;

    protected Value valueOf( PropertyBlock property )
    {
        // Make sure the value is loaded, even if it's of a "heavy" kind.
        propertyStore.ensureHeavy( property );
        return property.getType().value( property, propertyStore );
    }

    protected Iterable<PropertyBlock> properties( final RECORD relationship )
    {
        return () -> new PropertyBlockIterator( relationship );
    }

    @Override
    public void stop()
    {
        continueScanning = false;
    }

    @Override
    public PopulationProgress getProgress()
    {
        if ( totalCount > 0 )
        {
            return new PopulationProgress( count, totalCount );
        }

        // nothing to do 100% completed
        return PopulationProgress.DONE;
    }

    protected PrimitiveLongResourceIterator getEntityIdIterator()
    {
        return PrimitiveLongCollections.resourceIterator( new StoreIdIterator( store ), null );
    }

    protected class PropertyBlockIterator extends PrefetchingIterator<PropertyBlock>
    {
        private final Iterator<PropertyRecord> records;
        private Iterator<PropertyBlock> blocks = emptyIterator();

        PropertyBlockIterator( RECORD record )
        {
            long firstPropertyId = record.getNextProp();
            if ( firstPropertyId == Record.NO_NEXT_PROPERTY.intValue() )
            {
                records = emptyIterator();
            }
            else
            {
                records = propertyStore.getPropertyRecordChain( firstPropertyId ).iterator();
            }
        }

        @Override
        protected PropertyBlock fetchNextOrNull()
        {
            for ( ; ; )
            {
                if ( blocks.hasNext() )
                {
                    return blocks.next();
                }
                if ( !records.hasNext() )
                {
                    return null;
                }
                blocks = records.next().iterator();
            }
        }
    }
}
