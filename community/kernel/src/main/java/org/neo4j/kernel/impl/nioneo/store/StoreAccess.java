/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultLastCommittedTxIdSetter;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationDefaults;
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
    private final RecordStore<RelationshipTypeRecord> relTypeStore;
    private final RecordStore<PropertyRecord> propStore;
    // Transitive stores
    private final RecordStore<DynamicRecord> stringStore, arrayStore;
    private final RecordStore<PropertyIndexRecord> propIndexStore;
    private final RecordStore<DynamicRecord> typeNameStore;
    private final RecordStore<DynamicRecord> propKeyStore;
    // internal state
    private boolean closeable;
    private NeoStore neoStore;

    public StoreAccess( InternalAbstractGraphDatabase graphdb )
    {
        this( getNeoStoreFrom( graphdb ) );
    }

    private static NeoStore getNeoStoreFrom( InternalAbstractGraphDatabase graphdb )
    {
        return graphdb.getXaDataSourceManager().getNeoStoreDataSource().getNeoStore();
    }

    public StoreAccess( NeoStore store )
    {
        this( store.getNodeStore(), store.getRelationshipStore(), store.getPropertyStore(),
                store.getRelationshipTypeStore() );
        this.neoStore = store;
    }

    public StoreAccess( NodeStore nodeStore, RelationshipStore relStore, PropertyStore propStore,
            RelationshipTypeStore typeStore )
    {
        this.nodeStore = wrapStore( nodeStore );
        this.relStore = wrapStore( relStore );
        this.propStore = wrapStore( propStore );
        this.stringStore = wrapStore( propStore.getStringStore() );
        this.arrayStore = wrapStore( propStore.getArrayStore() );
        this.relTypeStore = wrapStore( typeStore );
        this.propIndexStore = wrapStore( propStore.getIndexStore() );
        this.typeNameStore = wrapStore( typeStore.getNameStore() );
        this.propKeyStore = wrapStore( propStore.getIndexStore().getNameStore() );
    }

    public StoreAccess( String path )
    {
        this( path, defaultParams() );
    }

    public StoreAccess( String path, Map<String, String> params )
    {
        this(
            new StoreFactory( new Config( new ConfigurationDefaults( GraphDatabaseSettings.class )
                                              .apply( requiredParams( params, path ) ) ), new DefaultIdGeneratorFactory(),
                              new DefaultFileSystemAbstraction(),
                              new DefaultLastCommittedTxIdSetter(), initLogger( path ),
                              new DefaultTxHook() ).attemptNewNeoStore( new File( path, "neostore" ).getAbsolutePath() ) );
        this.closeable = true;
    }

    private static StringLogger initLogger( String path )
    {
        StringLogger logger = StringLogger.logger( path );
        logger.logMessage( "Starting " + StoreAccess.class.getSimpleName() );
        return logger;
    }

    private static Map<String, String> requiredParams( Map<String, String> params, String path )
    {
        params = new HashMap<String, String>( params );
        params.put( "neo_store", new File( path, "neostore" ).getAbsolutePath() );
        return params;
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

    public RecordStore<RelationshipTypeRecord> getRelationshipTypeStore()
    {
        return relTypeStore;
    }

    public RecordStore<PropertyIndexRecord> getPropertyIndexStore()
    {
        return propIndexStore;
    }

    public RecordStore<DynamicRecord> getTypeNameStore()
    {
        return typeNameStore;
    }

    public RecordStore<DynamicRecord> getPropertyKeyStore()
    {
        return propKeyStore;
    }

    public final <P extends RecordStore.Processor> P applyToAll( P processor )
    {
        for( RecordStore<?> store : allStores() )
        {
            apply( processor, store );
        }
        return processor;
    }

    protected RecordStore<?>[] allStores()
    {
        if( propStore == null )
        {
            return new RecordStore<?>[]{ // no property stores
                                         nodeStore, relStore, relTypeStore, typeNameStore
            };
        }
        return new RecordStore<?>[] {
                nodeStore, relStore, propStore, stringStore, arrayStore, // basic
                relTypeStore, propIndexStore, typeNameStore, propKeyStore, // internal
                };
    }

    protected <R extends AbstractBaseRecord> RecordStore<R> wrapStore( RecordStore<R> store )
    {
        return store;
    }

    @SuppressWarnings( "unchecked" )
    protected void apply( RecordStore.Processor processor, RecordStore<?> store )
    {
        processor.applyFiltered( store, RecordStore.IN_USE );
    }

    private static Map<String, String> defaultParams()
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( GraphDatabaseSettings.nodestore_mapped_memory.name(), "20M" );
        params.put( GraphDatabaseSettings.nodestore_propertystore_mapped_memory.name(), "90M" );
        params.put( GraphDatabaseSettings.nodestore_propertystore_index_mapped_memory.name(), "1M" );
        params.put( GraphDatabaseSettings.nodestore_propertystore_index_mapped_memory.name(), "1M" );
        params.put( GraphDatabaseSettings.strings_mapped_memory.name(), "130M" );
        params.put( GraphDatabaseSettings.arrays_mapped_memory.name(), "130M" );
        params.put( GraphDatabaseSettings.relationshipstore_mapped_memory.name(), "100M" );
        // if on windows, default no memory mapping
        if ( GraphDatabaseSetting.osIsWindows() )
        {
            params.put( GraphDatabaseSettings.use_memory_mapped_buffers.name(), "false" );
        }
        params.put( GraphDatabaseSettings.rebuild_idgenerators_fast.name(), GraphDatabaseSetting.TRUE );
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
