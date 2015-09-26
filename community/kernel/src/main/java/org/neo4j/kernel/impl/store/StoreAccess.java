/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import java.io.File;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;

/**
 * Not thread safe (since DiffRecordStore is not thread safe), intended for
 * single threaded use.
 *
 * Make sure to call {@link #initialize()} after constructor has been run.
 */
public class StoreAccess
{
    // Top level stores
    private RecordStore<DynamicRecord> schemaStore;
    private RecordStore<NodeRecord> nodeStore;
    private RecordStore<RelationshipRecord> relStore;
    private RecordStore<RelationshipTypeTokenRecord> relationshipTypeTokenStore;
    private RecordStore<LabelTokenRecord> labelTokenStore;
    private RecordStore<DynamicRecord> nodeDynamicLabelStore;
    private RecordStore<PropertyRecord> propStore;
    // Transitive stores
    private RecordStore<DynamicRecord> stringStore, arrayStore;
    private RecordStore<PropertyKeyTokenRecord> propertyKeyTokenStore;
    private RecordStore<DynamicRecord> relationshipTypeNameStore;
    private RecordStore<DynamicRecord> labelNameStore;
    private RecordStore<DynamicRecord> propertyKeyNameStore;
    private RecordStore<RelationshipGroupRecord> relGroupStore;
    private final CountsAccessor counts;
    // internal state
    private boolean closeable;
    private final NeoStore neoStore;

    public StoreAccess( GraphDatabaseAPI graphdb )
    {
        this( getNeoStoreFrom( graphdb ) );
    }

    @SuppressWarnings( "deprecation" )
    private static NeoStore getNeoStoreFrom( GraphDatabaseAPI graphdb )
    {
        return graphdb.getDependencyResolver().resolveDependency( NeoStore.class );
    }

    public StoreAccess( NeoStore store )
    {
        this.neoStore = store;
        this.counts = store.getCounts();
    }

    public StoreAccess( PageCache pageCache, File storeDir )
    {
        this( new DefaultFileSystemAbstraction(), pageCache, storeDir );
    }

    public StoreAccess( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir )
    {
        this( fileSystem, pageCache, storeDir, new Config(), new Monitors() );
    }

    private StoreAccess( FileSystemAbstraction fileSystem, PageCache pageCache, File storeDir, Config config, Monitors monitors )
    {
        this( new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fileSystem ), pageCache,
                fileSystem, NullLogProvider.getInstance(), monitors ).newNeoStore( false ) );
        this.closeable = true;
    }

    /**
     * This method exists since {@link #wrapStore(RecordStore)} might depend on the existence of a variable
     * that gets set in a subclass' constructor <strong>after</strong> this constructor of {@link StoreAccess}
     * has been executed. I.e. a correct creation of a {@link StoreAccess} instance must be the creation of the
     * object plus a call to {@link #initialize()}.
     *
     * @return this
     */
    public StoreAccess initialize()
    {
        this.schemaStore = wrapStore( neoStore.getSchemaStore() );
        this.nodeStore = wrapStore( neoStore.getNodeStore() );
        this.relStore = wrapStore( neoStore.getRelationshipStore() );
        this.propStore = wrapStore( neoStore.getPropertyStore() );
        this.stringStore = wrapStore( neoStore.getPropertyStore().getStringStore() );
        this.arrayStore = wrapStore( neoStore.getPropertyStore().getArrayStore() );
        this.relationshipTypeTokenStore = wrapStore( neoStore.getRelationshipTypeTokenStore() );
        this.labelTokenStore = wrapStore( neoStore.getLabelTokenStore() );
        this.nodeDynamicLabelStore = wrapStore( wrapNodeDynamicLabelStore( neoStore.getNodeStore().getDynamicLabelStore() ) );
        this.propertyKeyTokenStore = wrapStore( neoStore.getPropertyStore().getPropertyKeyTokenStore() );
        this.relationshipTypeNameStore = wrapStore( neoStore.getRelationshipTypeTokenStore().getNameStore() );
        this.labelNameStore = wrapStore( neoStore.getLabelTokenStore().getNameStore() );
        this.propertyKeyNameStore = wrapStore( neoStore.getPropertyStore().getPropertyKeyTokenStore().getNameStore() );
        this.relGroupStore = wrapStore( neoStore.getRelationshipGroupStore() );
        return this;
    }

    public NeoStore getRawNeoStore()
    {
        return neoStore;
    }

    public RecordStore<DynamicRecord> getSchemaStore()
    {
        return schemaStore;
    }

    public RecordStore<NodeRecord> getNodeStore()
    {
        return nodeStore;
    }

    public RecordStore<RelationshipRecord> getRelationshipStore()
    {
        return relStore;
    }

    public RecordStore<RelationshipGroupRecord> getRelationshipGroupStore()
    {
        return relGroupStore;
    }

    public RecordStore<PropertyRecord> getPropertyStore()
    {
        return propStore;
    }

    public RecordStore<DynamicRecord> getStringStore()
    {
        return stringStore;
    }

    public RecordStore<DynamicRecord> getArrayStore()
    {
        return arrayStore;
    }

    public RecordStore<RelationshipTypeTokenRecord> getRelationshipTypeTokenStore()
    {
        return relationshipTypeTokenStore;
    }

    public RecordStore<LabelTokenRecord> getLabelTokenStore()
    {
        return labelTokenStore;
    }

    public RecordStore<DynamicRecord> getNodeDynamicLabelStore()
    {
        return nodeDynamicLabelStore;
    }

    public RecordStore<PropertyKeyTokenRecord> getPropertyKeyTokenStore()
    {
        return propertyKeyTokenStore;
    }

    public RecordStore<DynamicRecord> getRelationshipTypeNameStore()
    {
        return relationshipTypeNameStore;
    }

    public RecordStore<DynamicRecord> getLabelNameStore()
    {
        return labelNameStore;
    }

    public RecordStore<DynamicRecord> getPropertyKeyNameStore()
    {
        return propertyKeyNameStore;
    }

    public CountsAccessor getCounts()
    {
        return counts;
    }

    protected RecordStore<?>[] allStores()
    {
        if ( propStore == null )
        {
            // for when the property store isn't available (e.g. because the contained data in very sensitive)
            return new RecordStore<?>[]{ // no property stores
                    nodeStore, relStore,
                    relationshipTypeTokenStore, relationshipTypeNameStore,
                    labelTokenStore, labelNameStore, nodeDynamicLabelStore
            };
        }
        return new RecordStore<?>[]{
                schemaStore, nodeStore, relStore, propStore, stringStore, arrayStore,
                relationshipTypeTokenStore, propertyKeyTokenStore, labelTokenStore,
                relationshipTypeNameStore, propertyKeyNameStore, labelNameStore,
                nodeDynamicLabelStore
        };
    }

    private static RecordStore<DynamicRecord> wrapNodeDynamicLabelStore( RecordStore<DynamicRecord> store ) {
        return new DelegatingRecordStore<DynamicRecord>( store ) {
            @Override
            public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, DynamicRecord record)
                    throws FAILURE
            {
                processor.processLabelArrayWithOwner( this, record );
            }
        };
    }

    protected <R extends AbstractBaseRecord> RecordStore<R> wrapStore( RecordStore<R> store )
    {
        return store;
    }

    @SuppressWarnings("unchecked")
    protected <FAILURE extends Exception> void apply( RecordStore.Processor<FAILURE> processor, RecordStore<?> store )
            throws FAILURE
    {
        processor.applyFiltered( store, RecordStore.IN_USE );
    }

    public synchronized void close()
    {
        if ( closeable )
        {
            closeable = false;
            neoStore.close();
        }
    }
}
