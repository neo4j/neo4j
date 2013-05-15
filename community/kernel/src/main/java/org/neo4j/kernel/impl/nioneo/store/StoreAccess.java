/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Not thread safe (since DiffRecordStore is not thread safe), intended for
 * single threaded use.
 */
public class StoreAccess
{
    // Top level stores
    private final RecordStore<NodeRecord> nodeStore;
    private final RecordStore<RelationshipRecord> relStore;
    private final RecordStore<RelationshipTypeTokenRecord> relationshipTypeTokenStore;
    private final RecordStore<LabelTokenRecord> labelTokenStore;
    private final RecordStore<PropertyRecord> propStore;
    // Transitive stores
    private final RecordStore<DynamicRecord> stringStore, arrayStore;
    private final RecordStore<PropertyKeyTokenRecord> propertyKeyTokenStore;
    private final RecordStore<DynamicRecord> relationshipTypeNameStore;
    private final RecordStore<DynamicRecord> labelNameStore;
    private final RecordStore<DynamicRecord> propertyKeyNameStore;
    // internal state
    private boolean closeable;
    private NeoStore neoStore;

    public StoreAccess( GraphDatabaseAPI graphdb )
    {
        this( getNeoStoreFrom( graphdb ) );
    }

    @SuppressWarnings( "deprecation" )
    private static NeoStore getNeoStoreFrom( GraphDatabaseAPI graphdb )
    {
        return graphdb.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getNeoStoreDataSource().getNeoStore();
    }

    public StoreAccess( NeoStore store )
    {
        this( store.getNodeStore(), store.getRelationshipStore(), store.getPropertyStore(),
                store.getRelationshipTypeStore(), store.getLabelTokenStore() );
        this.neoStore = store;
    }

    public StoreAccess( NodeStore nodeStore, RelationshipStore relStore, PropertyStore propStore,
                        RelationshipTypeTokenStore typeStore, LabelTokenStore labelTokenStore )
    {
        this.nodeStore = wrapStore( nodeStore );
        this.relStore = wrapStore( relStore );
        this.propStore = wrapStore( propStore );
        this.stringStore = wrapStore( propStore.getStringStore() );
        this.arrayStore = wrapStore( propStore.getArrayStore() );
        this.relationshipTypeTokenStore = wrapStore( typeStore );
        this.labelTokenStore = wrapStore( labelTokenStore );
        this.propertyKeyTokenStore = wrapStore( propStore.getPropertyKeyTokenStore() );
        this.relationshipTypeNameStore = wrapStore( typeStore.getNameStore() );
        this.labelNameStore = wrapStore( labelTokenStore.getNameStore() );
        this.propertyKeyNameStore = wrapStore( propStore.getPropertyKeyTokenStore().getNameStore() );
    }

    public StoreAccess( String path )
    {
        this( path, defaultParams() );
    }

    public StoreAccess( FileSystemAbstraction fileSystem, String path )
    {
        this( fileSystem, path, defaultParams() );
    }
    
    public StoreAccess( String path, Map<String, String> params )
    {
        this( new DefaultFileSystemAbstraction(), path, params );
    }
    
    public StoreAccess( FileSystemAbstraction fileSystem, String path, Map<String, String> params )
    {
        this( new StoreFactory( new Config( requiredParams( params, path ) ),
                                new DefaultIdGeneratorFactory(),
                                new DefaultWindowPoolFactory(),
                                fileSystem, StringLogger.DEV_NULL,
                                new DefaultTxHook() ).attemptNewNeoStore( new File( path, "neostore" ) ) );
        this.closeable = true;
    }

    private static Map<String, String> requiredParams( Map<String, String> params, String path )
    {
        params = new HashMap<String, String>( params );
        params.put( "neo_store", new File( path, "neostore" ).getPath() );
        return params;
    }

    public NeoStore getRawNeoStore()
    {
        return neoStore;
    }

    public RecordStore<NodeRecord> getNodeStore()
    {
        return nodeStore;
    }

    public RecordStore<RelationshipRecord> getRelationshipStore()
    {
        return relStore;
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

    public final <F extends Exception, P extends RecordStore.Processor<F>> P applyToAll( P processor ) throws F
    {
        for ( RecordStore<?> store : allStores() )
        {
            apply( processor, store );
        }
        return processor;
    }

    protected RecordStore<?>[] allStores()
    {
        if ( propStore == null )
        {
            return new RecordStore<?>[]{ // no property stores
                    nodeStore, relStore, relationshipTypeTokenStore, relationshipTypeNameStore
            };
        }
        return new RecordStore<?>[]{
                nodeStore, relStore, propStore, stringStore, arrayStore, // basic
                relationshipTypeTokenStore, propertyKeyTokenStore, relationshipTypeNameStore, propertyKeyNameStore, // internal
        };
    }

    protected <R extends AbstractBaseRecord> RecordStore<R> wrapStore( RecordStore<R> store )
    {
        return store;
    }

    @SuppressWarnings("unchecked")
    protected <FAILURE extends Exception> void apply( RecordStore.Processor<FAILURE> processor, RecordStore<?> store ) throws FAILURE
    {
        processor.applyFiltered( store, RecordStore.IN_USE );
    }

    private static Map<String, String> defaultParams()
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( GraphDatabaseSettings.nodestore_mapped_memory_size.name(), "20M" );
        params.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory_size.name(), "90M" );
        params.put( GraphDatabaseSettings.nodestore_propertystore_index_mapped_memory_size.name(), "1M" );
        params.put( GraphDatabaseSettings.strings_mapped_memory_size.name(), "130M" );
        params.put( GraphDatabaseSettings.arrays_mapped_memory_size.name(), "130M" );
        params.put( GraphDatabaseSettings.relationshipstore_mapped_memory_size.name(), "100M" );
        // if on windows, default no memory mapping
        if ( GraphDatabaseSetting.osIsWindows() )
        {
            params.put( GraphDatabaseSettings.use_memory_mapped_buffers.name(), "false" );
        }
        params.put( GraphDatabaseSettings.rebuild_idgenerators_fast.name(), Settings.TRUE );
        return params;
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
