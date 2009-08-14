/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.batchinsert;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.impl.nioneo.store.DynamicRecord;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.NodeRecord;
import org.neo4j.impl.nioneo.store.NodeStore;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.impl.nioneo.store.PropertyRecord;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.Record;
import org.neo4j.impl.nioneo.store.RelationshipRecord;
import org.neo4j.impl.nioneo.store.RelationshipStore;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.impl.nioneo.store.RelationshipTypeStore;

public class BatchInserterImpl implements BatchInserter
{
    private final NeoStore neoStore;
    private final String storeDir;
    
    private final PropertyIndexHolder indexHolder;
    private final RelationshipTypeHolder typeHolder; 
    
    public BatchInserterImpl( String storeDir )
    {
        this( storeDir, Collections.EMPTY_MAP );
    }
    
    public BatchInserterImpl( String storeDir, 
        Map<String,String> stringParams )
    {
        Map<Object,Object> params = getDefaultParams();
        params.put( "use_memory_mapped_buffers", "false" );
        for ( Map.Entry<String,String> entry : stringParams.entrySet() )
        {
            params.put( entry.getKey(), entry.getValue() );
        }
        this.storeDir = storeDir;
        String store = fixPath( storeDir ); 
        params.put( "neo_store", store );

        // TODO: check if clean shutdown
        
        neoStore = new NeoStore( params );
        neoStore.makeStoreOk();
        PropertyIndexData[] indexes = 
            getPropertyIndexStore().getPropertyIndexes( 10000 );
        indexHolder = new PropertyIndexHolder( indexes );
        RelationshipTypeData[] types = 
            getRelationshipTypeStore().getRelationshipTypes();
        typeHolder = new RelationshipTypeHolder( types );
    }
    
    public long createNode( Map<String,Object> properties )
    {
        int nodeId = getNodeStore().nextId();
        NodeRecord nodeRecord = new NodeRecord( nodeId );
        nodeRecord.setInUse( true );
        nodeRecord.setCreated();
        nodeRecord.setNextProp( createPropertyChain( properties ) );
        getNodeStore().updateRecord( nodeRecord );
        return (nodeId & 0xFFFFFFFFL);
    }
    
    public void createNode( long id, Map<String,Object> properties )
    {
        if ( id < 0 || id > 0xFFFFFFFFL )
        {
            throw new IllegalArgumentException( "id=" + id );
        }
        int nodeId = (int) (id & 0xFFFFFFFF);
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
        int firstNodeId = (int) (node1 & 0xFFFFFFFF );
        int secondNodeId = (int) (node2 & 0xFFFFFFFF );
        NodeRecord firstNode = getNodeRecord( node1 );
        NodeRecord secondNode = getNodeRecord( node2 );
        int typeId = typeHolder.getTypeId( type.name() );
        if ( typeId == -1 )
        {
            typeId = createNewRelationshipType( type.name() );
        }
        int id = getRelationshipStore().nextId(); 
        RelationshipRecord record = new RelationshipRecord( id, firstNodeId,
            secondNodeId, typeId );
        record.setInUse( true );
        record.setCreated();
        connectRelationship( firstNode, secondNode, record );
        getNodeStore().updateRecord( firstNode );
        getNodeStore().updateRecord( secondNode );
        record.setNextProp( createPropertyChain( properties ) );
        getRelationshipStore().updateRecord( record );
        return id & 0xFFFFFFFFL;
    }
    
    private void connectRelationship( NodeRecord firstNode, 
        NodeRecord secondNode, RelationshipRecord rel )
    {
        rel.setFirstNextRel( firstNode.getNextRel() );
        rel.setSecondNextRel( secondNode.getNextRel() );
        if ( firstNode.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord nextRel = getRelationshipStore().getRecord( 
                firstNode.getNextRel() );
            if ( nextRel.getFirstNode() == firstNode.getId() )
            {
                nextRel.setFirstPrevRel( rel.getId() );
            }
            else if ( nextRel.getSecondNode() == firstNode.getId() )
            {
                nextRel.setSecondPrevRel( rel.getId() );
            }
            else
            {
                throw new RuntimeException( firstNode + " dont match "
                    + nextRel );
            }
            getRelationshipStore().updateRecord( nextRel );
        }
        if ( secondNode.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord nextRel = getRelationshipStore().getRecord(  
                secondNode.getNextRel() );
            if ( nextRel.getFirstNode() == secondNode.getId() )
            {
                nextRel.setFirstPrevRel( rel.getId() );
            }
            else if ( nextRel.getSecondNode() == secondNode.getId() )
            {
                nextRel.setSecondPrevRel( rel.getId() );
            }
            else
            {
                throw new RuntimeException( firstNode + " dont match "
                    + nextRel );
            }
            getRelationshipStore().updateRecord( nextRel );
        }
        firstNode.setNextRel( rel.getId() );
        secondNode.setNextRel( rel.getId() );
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
        int id = (int) (nodeId & 0xFFFFFFFF );
        return neoStore.getNodeStore().loadLightNode( id );
    }
    
    public Map<String,Object> getNodeProperties( long nodeId )
    {
        NodeRecord record = getNodeRecord( nodeId );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() ); 
        }
        return Collections.EMPTY_MAP;
    }
    
    public Iterable<Long> getRelationshipIds( long nodeId )
    {
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        int nextRel = nodeRecord.getNextRel();
        List<Long> ids = new ArrayList<Long>();
        while ( nextRel != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord relRecord = getRelationshipRecord( nextRel );
            ids.add( relRecord.getId() & 0xFFFFFFFFL );
            int firstNode = relRecord.getFirstNode();
            int secondNode = relRecord.getSecondNode();
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
                throw new RuntimeException( "GAH" );
            }
        }
        return ids;
    }
    
    public Iterable<SimpleRelationship> getRelationships( long nodeId )
    {
        NodeRecord nodeRecord = getNodeRecord( nodeId );
        int nextRel = nodeRecord.getNextRel();
        List<SimpleRelationship> rels = new ArrayList<SimpleRelationship>();
        while ( nextRel != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord relRecord = getRelationshipRecord( nextRel );
            RelationshipType type = new RelationshipTypeImpl( 
                typeHolder.getName( relRecord.getType() ) );
            rels.add( new SimpleRelationship( relRecord.getId(), 
                relRecord.getFirstNode(), relRecord.getSecondNode(), type ) );
            int firstNode = relRecord.getFirstNode();
            int secondNode = relRecord.getSecondNode();
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
                throw new RuntimeException( "GAH" );
            }
        }
        return rels;
    }
    
    public SimpleRelationship getRelatoinshipById( long relId )
    {
        RelationshipRecord record = getRelationshipRecord( relId );
        RelationshipType type = new RelationshipTypeImpl( 
            typeHolder.getName( record.getType() ) );
        return new SimpleRelationship( record.getId(), record.getFirstNode(), 
            record.getSecondNode(), type );
    }
    
    public Map getRelationshipProperties( long relId )
    {
        RelationshipRecord record = getRelationshipRecord( relId );
        if ( record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
        {
            return getPropertyChain( record.getNextProp() ); 
        }
        return Collections.EMPTY_MAP;
    }
    
    public void shutdown()
    {
        neoStore.close();
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
    
    public String toString()
    {
        return "EmbeddedBatchInserted[" + storeDir + "]";
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
    
    private int createPropertyChain( Map<String,Object> properties )
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
            int propertyId = propStore.nextId();
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
    
    private void deletePropertyChain( int propertyId )
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
    
    private Map<String,Object> getPropertyChain( int propertyId )
    {
        PropertyStore propStore = getPropertyStore();
        PropertyRecord propertyRecord = propStore.getRecord( propertyId );
        int nextProperty = -1;
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
        int keyId = idxStore.nextId();
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
        int id = typeStore.nextId();
        RelationshipTypeRecord record = new RelationshipTypeRecord( id );
        record.setInUse( true );
        record.setCreated();
        int typeBlockId = typeStore.nextBlockId();
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
            throw new IllegalArgumentException( "id=" + id );
        }
        return getNodeStore().getRecord( (int) (id & 0xFFFFFFFF) );
    }

    private RelationshipRecord getRelationshipRecord( long id )
    {
        if ( id < 0 || id >= getRelationshipStore().getHighId() )
        {
            throw new IllegalArgumentException( "id=" + id );
        }
        return getRelationshipStore().getRecord( (int) (id & 0xFFFFFFFF) );
    }

    private String fixPath( String dir )
    {
        File directories = new File( dir );
        if ( !directories.exists() )
        {
            if ( !directories.mkdirs() )
            {
                throw new RuntimeException( "Unable to create directory path["
                    + storeDir + "] for Neo store." );
            }
        }
        String fileSeparator = System.getProperty( "file.separator" );
        if ( "\\".equals( fileSeparator ) )
        {
            dir = dir.replace( '/', '\\' );
        }
        else if ( "/".equals( fileSeparator ) )
        {
            dir = dir.replace( '\\', '/' );
        }
        String store = dir + fileSeparator + "neostore";
        if ( !new File( store).exists() )
        {
            NeoStore.createStore( store );
        }
        return store;
    }
    
    public String getStore()
    {
        return storeDir;
    }
    
    public static Map<String,String> loadProperties( String file )
    {
        return EmbeddedNeo.loadConfigurations( file );
    }

    public long getReferenceNode()
    {
        if ( nodeExists( 0 ) )
        {
            return 0;
        }
        return -1;
    }
}