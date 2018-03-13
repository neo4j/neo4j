package org.neo4j.kernel.impl.transaction.state.storeview;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
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
import static org.neo4j.internal.kernel.api.schema.SchemaDescriptor.ANY_ENTITY_TOKEN;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

public abstract class PropertyAwareEntityStoreScan<RECORD extends PrimitiveRecord, FAILURE extends Exception> implements StoreScan<FAILURE>
{
    private final RecordStore<RECORD> store;
    private boolean continueScanning;
    private long count;
    private long totalCount;
    private final PropertyStore propertyStore;
    private final LockService locks;
    private final RECORD record;

    protected PropertyAwareEntityStoreScan( RecordStore<RECORD> store, LockService locks, PropertyStore propertyStore )
    {
        this.store = store;
        this.propertyStore = propertyStore;
        this.locks = locks;
        this.record = store.newRecord();
        this.totalCount = store.getHighId();
    }

    protected static boolean containsAnyEntityToken( int[] entityTokenFilter, long... entityTokens )
    {
        if ( Arrays.equals( entityTokenFilter, ANY_ENTITY_TOKEN ) )
        {
            return true;
        }
        for ( long candidate : entityTokens )
        {
            if ( ArrayUtils.contains( entityTokenFilter, Math.toIntExact( candidate ) ) )
            {
                return true;
            }
        }
        return false;
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
                try ( Lock ignored = locks.acquireRelationshipLock( id, LockService.LockType.READ_LOCK ) )
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
    public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, IndexEntryUpdate<?> update, long currentlyIndexedNodeId )
    {
        if ( update.getEntityId() <= currentlyIndexedNodeId )
        {
            updater.process( update );
        }
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
