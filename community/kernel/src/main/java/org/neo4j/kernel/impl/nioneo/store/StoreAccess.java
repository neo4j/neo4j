/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;

/**
 * Not thread safe (since DiffRecordStore is not thread safe), intended for
 * single threaded use.
 */
public class StoreAccess
{
    private final RecordStore<NodeRecord> nodeStore;
    private final RecordStore<RelationshipRecord> relStore;
    private final RecordStore<PropertyRecord> propStore;
    private final RecordStore<DynamicRecord> stringStore, arrayStore;
    private final RecordStore<RelationshipTypeRecord> relTypeStore;
    private final RecordStore<PropertyIndexRecord> propIndexStore;
    private final RecordStore<DynamicRecord> typeNames;
    private final RecordStore<DynamicRecord> propKeys;
    private boolean closable = false;

    public StoreAccess( String path )
    {
        this( path, defaultParams() );
    }

    public StoreAccess( String path, Map<Object, Object> params )
    {
        params.put( FileSystemAbstraction.class, CommonFactories.defaultFileSystemAbstraction() );
        // these need to be made ok
        NodeStore nodeStore = new NodeStore( path + "/neostore.nodestore.db", params );
        RelationshipStore relStore = new RelationshipStore( path + "/neostore.relationshipstore.db", params );
        PropertyStore propStore = null;
        RelationshipTypeStore relTypeStore = new RelationshipTypeStore( path + "/neostore.relationshiptypestore.db",
                params, IdType.RELATIONSHIP_TYPE );
        this.nodeStore = nodeStore;
        this.relStore = relStore;
        this.relTypeStore = relTypeStore;
        this.typeNames = wrapStore( relTypeStore.getNameStore() );
        if ( new File( path + "/neostore.propertystore.db" ).exists() )
        {
            propStore = new PropertyStore( path + "/neostore.propertystore.db", params );
            this.propStore = propStore;
            this.propIndexStore = wrapStore( propStore.getIndexStore() );
            this.propKeys = wrapStore( propStore.getIndexStore().getKeyStore() );
            this.stringStore = wrapStore( propStore.getStringStore() );
            this.arrayStore = wrapStore( propStore.getArrayStore() );
        }
        else
        {
            this.propStore = null;
            this.propIndexStore = null;
            this.propKeys = null;
            this.stringStore = null;
            this.arrayStore = null;
        }
        this.closable = true;
        nodeStore.makeStoreOk();
        relStore.makeStoreOk();
        if ( propStore != null ) propStore.makeStoreOk();
        relTypeStore.makeStoreOk();
    }

    public StoreAccess( NeoStore store )
    {
        this( store.getNodeStore(), store.getRelationshipStore(), store.getPropertyStore(),
                store.getRelationshipTypeStore() );
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
        this.typeNames = wrapStore( typeStore.getNameStore() );
        this.propKeys = wrapStore( propStore.getIndexStore().getKeyStore() );
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
        return typeNames;
    }

    public RecordStore<DynamicRecord> getPropertyKeyStore()
    {
        return propKeys;
    }

    public void close()
    {
        if ( closable ) try
        {
            nodeStore.close();
            relStore.close();
            if ( propStore != null ) propStore.close();
        }
        finally
        {
            closable = false;
        }
    }

    public final <P extends RecordStore.Processor> P apply( P processor )
    {
        for ( RecordStore<?> store : stores() )
            apply( processor, store );
        return processor;
    }

    protected RecordStore<?>[] stores()
    {
        return new RecordStore<?>[] { nodeStore, relStore, propStore, stringStore, arrayStore, typeNames, propKeys };
    }

    protected <R extends AbstractBaseRecord> RecordStore<R> wrapStore( RecordStore<R> store )
    {
        return store;
    }

    @SuppressWarnings( "unchecked" )
    protected void apply( RecordStore.Processor processor, RecordStore<?> store )
    {
        processor.apply( store, RecordStore.IN_USE );
    }

    private static Map<Object, Object> defaultParams()
    {
        Map<Object, Object> params = new HashMap<Object, Object>();
        params.put( "neostore.nodestore.db.mapped_memory", "20M" );
        params.put( "neostore.propertystore.db.mapped_memory", "90M" );
        params.put( "neostore.propertystore.db.index.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.index.keys.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.strings.mapped_memory", "130M" );
        params.put( "neostore.propertystore.db.arrays.mapped_memory", "130M" );
        params.put( "neostore.relationshipstore.db.mapped_memory", "100M" );
        // if on windows, default no memory mapping
        String nameOs = System.getProperty( "os.name" );
        if ( nameOs.startsWith( "Windows" ) )
        {
            params.put( "use_memory_mapped_buffers", "false" );
        }
        params.put( Config.REBUILD_IDGENERATORS_FAST, "true" );

        params.put( IdGeneratorFactory.class, new CommonFactories.DefaultIdGeneratorFactory() );
        return params;
    }
}
