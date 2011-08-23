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
package org.neo4j.kernel.impl.batchinsert;

import static java.lang.Boolean.parseBoolean;
import static org.neo4j.kernel.Config.ALLOW_STORE_UPGRADE;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.AutoConfigurator;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexData;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

public class BatchInserterImpl implements BatchInserter
{
    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
    private static final long MAX_RELATIONSHIP_ID = IdType.RELATIONSHIP.getMaxValue();

    private final NeoStore neoStore;
    private final IndexStore indexStore;
    private final String storeDir;

    private final PropertyIndexHolder indexHolder;
    private final RelationshipTypeHolder typeHolder;

    private final BatchGraphDatabaseImpl graphDbService;
    private final IdGeneratorFactory idGeneratorFactory;

    private final StringLogger msgLog;

    public BatchInserterImpl( String storeDir )
    {
        this( storeDir, Collections.<String, String>emptyMap() );
    }

    public BatchInserterImpl( String storeDir,
        Map<String,String> stringParams )
    {
        rejectAutoUpgrade( stringParams );
        msgLog = StringLogger.getLogger( storeDir );
        Map<Object,Object> params = getDefaultParams();
        params.put( Config.USE_MEMORY_MAPPED_BUFFERS, "false" );
        boolean dump = Boolean.parseBoolean( stringParams.get( Config.DUMP_CONFIGURATION ) );
        new AutoConfigurator( storeDir, false, dump ).configure( params );
        for ( Map.Entry<String,String> entry : stringParams.entrySet() )
        {
            params.put( entry.getKey(), entry.getValue() );
        }
        this.storeDir = storeDir;
        this.idGeneratorFactory = CommonFactories.defaultIdGeneratorFactory();
        params.put( IdGeneratorFactory.class, idGeneratorFactory );
        params.put( FileSystemAbstraction.class, CommonFactories.defaultFileSystemAbstraction() );
        String store = fixPath( storeDir, params );
        params.put( "neo_store", store );
        if ( dump )
        {
            Config.dumpConfiguration( params );
        }
        msgLog.logMessage( Thread.currentThread() + " Starting BatchInserter(" + this + ")" );
        neoStore = new NeoStore( params );
        if ( !neoStore.isStoreOk() )
        {
            throw new IllegalStateException( storeDir + " store is not cleanly shutdown." );
        }
        neoStore.makeStoreOk();
        PropertyIndexData[] indexes =
            getPropertyIndexStore().getPropertyIndexes( 10000 );
        indexHolder = new PropertyIndexHolder( indexes );
        RelationshipTypeData[] types =
            getRelationshipTypeStore().getRelationshipTypes();
        typeHolder = new RelationshipTypeHolder( types );
        graphDbService = new BatchGraphDatabaseImpl( this );
        indexStore = new IndexStore( storeDir );
    }

    private void rejectAutoUpgrade( Map<String, String> stringParams )
    {
        if ( parseBoolean( stringParams.get( ALLOW_STORE_UPGRADE ) ) )
        {
            throw new IllegalArgumentException( "Batch inserter is not allowed to do upgrade of a store" +
            		", use " + EmbeddedGraphDatabase.class.getSimpleName() + " instead" );
        }
    }

    public long createNode( Map<String,Object> properties )
    {
        long nodeId = getNodeStore().nextId();
        NodeRecord nodeRecord = new NodeRecord( nodeId );
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
        nodeRecord.setNextProp( createPropertyChain( properties ) );
        getNodeStore().updateRecord( nodeRecord );
        return nodeId;
    }

    public void createNode( long id, Map<String,Object> properties )
    {
        if ( id < 0 || id > MAX_NODE_ID )
        {
            throw new IllegalArgumentException( "id=" + id );
        }
        if ( id == IdGeneratorImpl.INTEGER_MINUS_ONE )
        {
            throw new IllegalArgumentException( "id " + id + " is reserved for internal use" );
        }
        long nodeId = id;
        NodeStore nodeStore = neoStore.getNodeStore();
        if ( neoStore.getNodeStore().loadLightNode( nodeId ) )
        {
            throw new IllegalArgumentException( "id=" + id + " already in use" );
        }
        long highId = nodeStore.getHighId();
        if ( highId <= id )
        {
            nodeStore.setHighId( nodeId + 1 );
        }
        NodeRecord nodeRecord = new NodeRecord( nodeId );
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
        nodeRecord.setNextProp( createPropertyChain( properties ) );
        getNodeStore().updateRecord( nodeRecord );
    }

    public long createRelationship( long node1, long node2, RelationshipType
        type, Map<String,Object> properties )
    {
        NodeRecord firstNode = getNodeRecord( node1 );
        NodeRecord secondNode = getNodeRecord( node2 );
        int typeId = typeHolder.getTypeId( type.name() );
        if ( typeId == -1 )
        {
            typeId = createNewRelationshipType( type.name() );
        }
        long id = getRelationshipStore().nextId();
        RelationshipRecord record = new RelationshipRecord( id, node1, node2, typeId );
        record.setInUse( true );
        record.setCreated();
        connectRelationship( firstNode, secondNode, record );
        getNodeStore().updateRecord( firstNode );
        getNodeStore().updateRecord( secondNode );
        record.setNextProp( createPropertyChain( properties ) );
        getRelationshipStore().updateRecord( record );
        return id;
    }

    private void connectRelationship( NodeRecord firstNode,
            NodeRecord secondNode, RelationshipRecord rel )
    {
        assert firstNode.getNextRel() != rel.getId();
        assert secondNode.getNextRel() != rel.getId();
        rel.setFirstNextRel( firstNode.getNextRel() );
        rel.setSecondNextRel( secondNode.getNextRel() );
        connect( firstNode, rel );
        connect( secondNode, rel );
        firstNode.setNextRel( rel.getId() );
        secondNode.setNextRel( rel.getId() );
    }

    private void connect( NodeRecord node, RelationshipRecord rel )
    {
        if ( node.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord nextRel = getRelationshipStore().getRecord( node.getNextRel() );
            boolean changed = false;
            if ( nextRel.getFirstNode() == node.getId() )
            {
                nextRel.setFirstPrevRel( rel.getId() );
                changed = true;
            }
            if ( nextRel.getSecondNode() == node.getId() )
            {
                nextRel.setSecondPrevRel( rel.getId() );
                changed = true;
            }
            if ( !changed )
            {
                throw new InvalidRecordException( node + " dont match " + nextRel );
            }
            getRelationshipStore().updateRecord( nextRel );
        }
    }

    public void setNodeProperties( long node, Map<String,Object> properties )
    {
        NodeRecord record = getNodeRecord( node );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            deletePropertyChain( record.getNextProp() );
        }
        record.setNextProp( createPropertyChain( properties ) );
        getNodeStore().updateRecord( record );
    }

    public void setRelationshipProperties( long rel,
        Map<String,Object> properties )
    {
        RelationshipRecord record = getRelationshipRecord( rel );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            deletePropertyChain( record.getNextProp() );
        }
        record.setNextProp( createPropertyChain( properties ) );
        getRelationshipStore().updateRecord( record );
    }

    public boolean nodeExists( long nodeId )
    {
        return neoStore.getNodeStore().loadLightNode( nodeId );
    }

    public Map<String,Object> getNodeProperties( long nodeId )
    {
        NodeRecord record = getNodeRecord( nodeId );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() );
        }
        return Collections.emptyMap();
    }

    public Iterable<Long> getRelationshipIds( long nodeId )
    {
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        long nextRel = nodeRecord.getNextRel();
        List<Long> ids = new ArrayList<Long>();
        while ( nextRel != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord relRecord = getRelationshipRecord( nextRel );
            ids.add( relRecord.getId() );
            long firstNode = relRecord.getFirstNode();
            long secondNode = relRecord.getSecondNode();
            if ( firstNode == nodeId )
            {
                nextRel = relRecord.getFirstNextRel();
            }
            else if ( secondNode == nodeId )
            {
                nextRel = relRecord.getSecondNextRel();
            }
            else
            {
                throw new InvalidRecordException( "Node[" + nodeId +
                    "] not part of firstNode[" + firstNode +
                    "] or secondNode[" + secondNode + "]" );
            }
        }
        return ids;
    }

    public Iterable<SimpleRelationship> getRelationships( long nodeId )
    {
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        long nextRel = nodeRecord.getNextRel();
        List<SimpleRelationship> rels = new ArrayList<SimpleRelationship>();
        while ( nextRel != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord relRecord = getRelationshipRecord( nextRel );
            RelationshipType type = new RelationshipTypeImpl(
                typeHolder.getName( relRecord.getType() ) );
            rels.add( new SimpleRelationship( relRecord.getId(),
                relRecord.getFirstNode(), relRecord.getSecondNode(), type ) );
            long firstNode = relRecord.getFirstNode();
            long secondNode = relRecord.getSecondNode();
            if ( firstNode == nodeId )
            {
                nextRel = relRecord.getFirstNextRel();
            }
            else if ( secondNode == nodeId )
            {
                nextRel = relRecord.getSecondNextRel();
            }
            else
            {
                throw new InvalidRecordException( "Node[" + nodeId +
                    "] not part of firstNode[" + firstNode +
                    "] or secondNode[" + secondNode + "]" );
            }
        }
        return rels;
    }

    public SimpleRelationship getRelationshipById( long relId )
    {
        RelationshipRecord record = getRelationshipRecord( relId );
        RelationshipType type = new RelationshipTypeImpl(
            typeHolder.getName( record.getType() ) );
        return new SimpleRelationship( record.getId(), record.getFirstNode(),
            record.getSecondNode(), type );
    }

    public Map<String,Object> getRelationshipProperties( long relId )
    {
        RelationshipRecord record = getRelationshipRecord( relId );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() );
        }
        return Collections.emptyMap();
    }

    public void shutdown()
    {
        graphDbService.clearCaches();
        neoStore.close();
        msgLog.logMessage( Thread.currentThread() + " Clean shutdown on BatchInserter(" + this + ")", true );
        StringLogger.close( storeDir );
    }

    private Map<Object,Object> getDefaultParams()
    {
        Map<Object,Object> params = new HashMap<Object,Object>();
        params.put( "neostore.nodestore.db.mapped_memory", "20M" );
        params.put( "neostore.propertystore.db.mapped_memory", "90M" );
        params.put( "neostore.propertystore.db.index.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.index.keys.mapped_memory", "1M" );
        params.put( "neostore.propertystore.db.strings.mapped_memory", "130M" );
        params.put( "neostore.propertystore.db.arrays.mapped_memory", "130M" );
        params.put( "neostore.relationshipstore.db.mapped_memory", "50M" );
        return params;
    }

    @Override
    public String toString()
    {
        return "EmbeddedBatchInserter[" + storeDir + "]";
    }

    private static class RelationshipTypeImpl implements RelationshipType
    {
        private final String name;

        RelationshipTypeImpl( String name )
        {
            this.name = name;
        }

        public String name()
        {
            return name;
        }
    }

    private long createPropertyChain( Map<String,Object> properties )
    {
        if ( properties == null )
        {
            return Record.NO_NEXT_PROPERTY.intValue();
        }
        PropertyStore propStore = getPropertyStore();
        List<PropertyRecord> propRecords = new ArrayList<PropertyRecord>();
        PropertyRecord prevRecord = null;
        for ( Entry<String,Object> entry : properties.entrySet() )
        {
            int keyId = indexHolder.getKeyId( entry.getKey() );
            if ( keyId == -1 )
            {
                keyId = createNewPropertyIndex( entry.getKey() );
            }
            long propertyId = propStore.nextId();
            PropertyRecord propertyRecord = new PropertyRecord( propertyId );
            propertyRecord.setInUse( true );
            propertyRecord.setCreated();
            propertyRecord.setKeyIndexId( keyId );
            propStore.encodeValue( propertyRecord, entry.getValue() );
            if ( prevRecord != null )
            {
                prevRecord.setPrevProp( propertyId );
                propertyRecord.setNextProp( prevRecord.getId() );
            }
            propRecords.add( propertyRecord );
            prevRecord = propertyRecord;
        }
        // reverse order results in forward update to store
        for ( int i = propRecords.size() - 1; i >=0; i-- )
        {
            propStore.updateRecord( propRecords.get( i ) );
        }
        if ( prevRecord != null )
        {
            return prevRecord.getId();
        }
        return Record.NO_NEXT_PROPERTY.intValue();
    }

    private void deletePropertyChain( long propertyId )
    {
        PropertyStore propStore = getPropertyStore();
        PropertyRecord propertyRecord = propStore.getRecord( propertyId );
        propertyRecord.setInUse( false );
        for ( DynamicRecord record : propertyRecord.getValueRecords() )
        {
            record.setInUse( false );
        }
        propStore.updateRecord( propertyRecord );
    }

    private Map<String,Object> getPropertyChain( long propertyId )
    {
        PropertyStore propStore = getPropertyStore();
        PropertyRecord propertyRecord = propStore.getRecord( propertyId );
        long nextProperty = -1;
        Map<String,Object> properties = new HashMap<String,Object>();
        do
        {
            nextProperty = propertyRecord.getNextProp();
            propStore.makeHeavy( propertyRecord );
            String key = indexHolder.getStringKey(
                propertyRecord.getKeyIndexId() );
            Object value = propStore.getValue( propertyRecord );
            properties.put( key, value );
            if ( nextProperty != Record.NO_NEXT_PROPERTY.intValue() )
            {
                propertyRecord =
                    propStore.getRecord( propertyRecord.getNextProp() );
            }
        } while ( nextProperty != Record.NO_NEXT_PROPERTY.intValue() );
        return properties;
    }

    private int createNewPropertyIndex( String stringKey )
    {
        PropertyIndexStore idxStore = getPropertyIndexStore();
        int keyId = (int) idxStore.nextId();
        PropertyIndexRecord record = new PropertyIndexRecord( keyId );
        record.setInUse( true );
        record.setCreated();
        int keyBlockId = idxStore.nextKeyBlockId();
        record.setKeyBlockId( keyBlockId );
        int length = stringKey.length();
        char[] chars = new char[length];
        stringKey.getChars( 0, length, chars, 0 );
        Collection<DynamicRecord> keyRecords =
            idxStore.allocateKeyRecords( keyBlockId, chars );
        for ( DynamicRecord keyRecord : keyRecords )
        {
            record.addKeyRecord( keyRecord );
        }
        idxStore.updateRecord( record );
        indexHolder.addPropertyIndex( stringKey, keyId );
        return keyId;
    }

    private int createNewRelationshipType( String name )
    {
        RelationshipTypeStore typeStore = getRelationshipTypeStore();
        int id = (int) typeStore.nextId();
        RelationshipTypeRecord record = new RelationshipTypeRecord( id );
        record.setInUse( true );
        record.setCreated();
        int typeBlockId = (int) typeStore.nextBlockId();
        record.setTypeBlock( typeBlockId );
        int length = name.length();
        char[] chars = new char[length];
        name.getChars( 0, length, chars, 0 );
        Collection<DynamicRecord> typeRecords =
            typeStore.allocateTypeNameRecords( typeBlockId, chars );
        for ( DynamicRecord typeRecord : typeRecords )
        {
            record.addTypeRecord( typeRecord );
        }
        typeStore.updateRecord( record );
        typeHolder.addRelationshipType( name, id );
        return id;
    }

    private NodeStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    private PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    private PropertyIndexStore getPropertyIndexStore()
    {
        return getPropertyStore().getIndexStore();
    }

    private RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    private RelationshipTypeStore getRelationshipTypeStore()
    {
        return neoStore.getRelationshipTypeStore();
    }

    private NodeRecord getNodeRecord( long id )
    {
        if ( id < 0 || id >= getNodeStore().getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return getNodeStore().getRecord( id );
    }

    private RelationshipRecord getRelationshipRecord( long id )
    {
        if ( id < 0 || id >= getRelationshipStore().getHighId() )
        {
            throw new NotFoundException( "id=" + id );
        }
        return getRelationshipStore().getRecord( id );
    }

    private String fixPath( String dir, Map<?,?> config )
    {
        File directories = new File( dir );
        if ( !directories.exists() )
        {
            if ( !directories.mkdirs() )
            {
                throw new UnderlyingStorageException(
                    "Unable to create directory path["
                    + storeDir + "] for Neo4j kernel store." );
            }
        }
        dir = FileUtils.fixSeparatorsInPath( dir );
        String fileSeparator = System.getProperty( "file.separator" );
        String store = dir + fileSeparator + "neostore";
        if ( !new File( store ).exists() )
        {
            NeoStore.createStore( store, config );
        }
        return store;
    }

    public String getStore()
    {
        return storeDir;
    }

    public static Map<String,String> loadProperties( String file )
    {
        return EmbeddedGraphDatabase.loadConfigurations( file );
    }

    public long getReferenceNode()
    {
        if ( nodeExists( 0 ) )
        {
            return 0;
        }
        return -1;
    }

    public GraphDatabaseService getGraphDbService()
    {
        return graphDbService;
    }

    public IndexStore getIndexStore()
    {
        return this.indexStore;
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return idGeneratorFactory;
    }
}
