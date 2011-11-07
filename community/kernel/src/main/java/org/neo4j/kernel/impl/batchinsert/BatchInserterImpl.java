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
import static org.neo4j.kernel.impl.nioneo.store.PropertyStore.encodeString;

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
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexData;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
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

    @Override
    public boolean nodeHasProperty( long node, String propertyName )
    {
        return primitiveHasProperty( getNodeRecord( node ), propertyName );
    }

    @Override
    public boolean relationshipHasProperty( long relationship,
            String propertyName )
    {
        return primitiveHasProperty( getRelationshipRecord( relationship ),
                propertyName );
    }

    @Override
    public void setNodeProperty( long node, String propertyName,
            Object propertyValue )
    {
        NodeRecord nodeRec = getNodeRecord( node );
        if ( setPrimitiveProperty( nodeRec, propertyName, propertyValue ) )
        {
            getNodeStore().updateRecord( nodeRec );
        }
    }

    @Override
    public void setRelationshipProperty( long relationship,
            String propertyName, Object propertyValue )
    {
        RelationshipRecord relRec = getRelationshipRecord( relationship );
        if ( setPrimitiveProperty( relRec, propertyName, propertyValue ) )
        {
            getRelationshipStore().updateRecord( relRec );
        }
    }

    @Override
    public void removeNodeProperty( long node, String propertyName )
    {
        NodeRecord nodeRec = getNodeRecord( node );
        if ( removePrimitiveProperty( nodeRec, propertyName ) )
        {
            getNodeStore().updateRecord( nodeRec );
        }
    }

    @Override
    public void removeRelationshipProperty( long relationship,
            String propertyName )
    {
        RelationshipRecord relationshipRec = getRelationshipRecord( relationship );
        if ( removePrimitiveProperty( relationshipRec, propertyName ) )
        {
            getRelationshipStore().updateRecord( relationshipRec );
        }
    }

    private boolean removePrimitiveProperty( PrimitiveRecord primitive,
            String property )
    {
        PropertyRecord current = null;
        PropertyBlock target = null;
        long nextProp = primitive.getNextProp();
        int propIndex = indexHolder.getKeyId( property );
        if ( nextProp == Record.NO_NEXT_PROPERTY.intValue() || propIndex == -1 )
        {
            // No properties or no one has that property, nothing changed
            return false;
        }
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            current = getPropertyStore().getRecord( nextProp );
            if ( ( target = current.removePropertyBlock( propIndex ) ) != null )
            {
                if ( target.isLight() )
                {
                    getPropertyStore().makeHeavy( target );
                }
                for ( DynamicRecord dynRec : target.getValueRecords() )
                {
                    current.addDeletedRecord( dynRec );
                }
                break;
            }
            nextProp = current.getNextProp();
        }
        if ( current.size() > 0 )
        {
            getPropertyStore().updateRecord( current );
            return false;
        }
        else
        {
            return unlinkPropertyRecord( current, primitive );
        }
    }

    private boolean unlinkPropertyRecord( PropertyRecord propRecord,
            PrimitiveRecord primitive )
    {
        assert propRecord.size() == 0;
        boolean primitiveChanged = false;
        long prevProp = propRecord.getPrevProp();
        long nextProp = propRecord.getNextProp();
        if ( primitive.getNextProp() == propRecord.getId() )
        {
            assert propRecord.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue() : propRecord
                                                                                        + " for "
                                                                                        + primitive;
            primitive.setNextProp( nextProp );
            primitiveChanged = true;
        }
        if ( prevProp != Record.NO_PREVIOUS_PROPERTY.intValue() )
        {
            PropertyRecord prevPropRecord = getPropertyStore().getRecord(
                    prevProp );
            assert prevPropRecord.inUse() : prevPropRecord + "->" + propRecord
                                            + " for " + primitive;
            prevPropRecord.setNextProp( nextProp );
            getPropertyStore().updateRecord( prevPropRecord );
        }
        if ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord nextPropRecord = getPropertyStore().getRecord(
                    nextProp );
            assert nextPropRecord.inUse() : propRecord + "->" + nextPropRecord
                                            + " for " + primitive;
            nextPropRecord.setPrevProp( prevProp );
            getPropertyStore().updateRecord( nextPropRecord );
        }
        propRecord.setInUse( false );
        /*
         *  The following two are not needed - the above line does all the work (PropertyStore
         *  does not write out the prev/next for !inUse records). It is nice to set this
         *  however to check for consistency when assertPropertyChain().
         */
        propRecord.setPrevProp( Record.NO_PREVIOUS_PROPERTY.intValue() );
        propRecord.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
        getPropertyStore().updateRecord( propRecord );
        return primitiveChanged;
    }

    /**
     * @return true if the passed primitive needs updating in the store.
     */
    private boolean setPrimitiveProperty( PrimitiveRecord primitive,
            String name,
            Object value )
    {
        boolean result = false;
        long nextProp = primitive.getNextProp();
        int index = indexHolder.getKeyId( name );

        if ( index == -1 )
        {
            index = createNewPropertyIndex( name );
        }
        PropertyBlock block = new PropertyBlock();
        getPropertyStore().encodeValue( block, index, value );
        int size = block.getSize();

        /*
         * current is the current record traversed
         * thatFits is the earliest record that can host the block
         * thatHas is the record that already has a block for this index
         */
        PropertyRecord current = null, thatFits = null, thatHas = null;
        /*
         * We keep going while there are records or until we both found the
         * property if it exists and the place to put it, if exists.
         */
        while ( !( nextProp == Record.NO_NEXT_PROPERTY.intValue() || ( thatHas != null && thatFits != null ) ) )
        {
            current = getPropertyStore().getRecord( nextProp );
            /*
             * current.getPropertyBlock() is cheap but not free. If we already
             * have found thatHas, then we can skip this lookup.
             */
            if ( thatHas == null && current.getPropertyBlock( index ) != null )
            {
                thatHas = current;
                PropertyBlock removed = thatHas.removePropertyBlock( index );
                if ( removed.isLight() )
                {
                    getPropertyStore().makeHeavy( removed );
                    for ( DynamicRecord dynRec : removed.getValueRecords() )
                    {
                        thatHas.addDeletedRecord( dynRec );
                    }
                }
                getPropertyStore().updateRecord( thatHas );
            }
            /*
             * We check the size after we remove - potentially we can put in the same record.
             *
             * current.size() is cheap but not free. If we already found somewhere
             * where it fits, no need to look again.
             */
            if ( thatFits == null
                 && ( PropertyType.getPayloadSize() - current.size() >= size ) )
            {
                thatFits = current;
            }
            nextProp = current.getNextProp();
        }
        /*
         * thatHas is of no importance here. We know that the block is definitely not there.
         * However, we can be sure that if the property existed, thatHas is not null and does
         * not contain the block.
         *
         * thatFits is interesting. If null, we need to create a new record and link, otherwise
         * just add the block there.
         */
        if ( thatFits == null )
        {
            thatFits = new PropertyRecord( getPropertyStore().nextId() );

            if ( primitive.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
            {
                PropertyRecord first = getPropertyStore().getRecord(
                        primitive.getNextProp() );
                thatFits.setNextProp( first.getId() );
                first.setPrevProp( thatFits.getId() );
                getPropertyStore().updateRecord( first );
                result = true;
            }
            primitive.setNextProp( thatFits.getId() );
        }
        thatFits.addPropertyBlock( block );
        getPropertyStore().updateRecord( thatFits );
        return result;
    }

    private boolean primitiveHasProperty( PrimitiveRecord record,
            String propertyName )
    {
        long nextProp = record.getNextProp();
        int propertyIndex = indexHolder.getKeyId( propertyName );
        if (nextProp == Record.NO_NEXT_PROPERTY.intValue() || propertyIndex == -1)
        {
            return false;
        }

        PropertyRecord current = null;
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            current = getPropertyStore().getRecord( nextProp );
            if ( current.getPropertyBlock( propertyIndex ) != null )
            {
                return true;
            }
            nextProp = current.getNextProp();
        }
        return false;
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
            /*
             * Batch inserter does not make any attempt to maintain the store's
             * integrity. It makes sense however to keep some things intact where
             * the cost is relatively low. So here, when we delete the property
             * chain we first make sure that the node record (or the relationship
             * record below) does not point anymore to the deleted properties. This
             * way, if during creation, something goes wrong, it will not have the properties
             * expected instead of throwing invalid record exceptions.
             */
            record.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
            getNodeStore().updateRecord( record );
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
            /*
             * See setNodeProperties above for an explanation of what goes on
             * here
             */
            record.setNextProp( Record.NO_NEXT_PROPERTY.intValue() );
            getRelationshipStore().updateRecord( record );
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
        if ( properties == null || properties.isEmpty() )
        {
            return Record.NO_NEXT_PROPERTY.intValue();
        }
        PropertyStore propStore = getPropertyStore();
        List<PropertyRecord> propRecords = new ArrayList<PropertyRecord>();
        PropertyRecord currentRecord = new PropertyRecord( propStore.nextId() );
        currentRecord.setInUse( true );
        currentRecord.setCreated();
        propRecords.add( currentRecord );
        for ( Entry<String,Object> entry : properties.entrySet() )
        {
            int keyId = indexHolder.getKeyId( entry.getKey() );
            if ( keyId == -1 )
            {
                keyId = createNewPropertyIndex( entry.getKey() );
            }

            PropertyBlock block = new PropertyBlock();
            propStore.encodeValue( block, keyId, entry.getValue() );
            if ( currentRecord.size() + block.getSize() > PropertyType.getPayloadSize() )
            {
                // Here it means the current block is done for
                PropertyRecord prevRecord = currentRecord;
                // Create new record
                long propertyId = propStore.nextId();
                currentRecord = new PropertyRecord( propertyId );
                currentRecord.setInUse( true );
                currentRecord.setCreated();
                // Set up links
                prevRecord.setNextProp( propertyId );
                currentRecord.setPrevProp( prevRecord.getId() );
                propRecords.add( currentRecord );
                // Now current is ready to start picking up blocks
            }
            currentRecord.addPropertyBlock( block );
        }
        /*
         * Add the property records in reverse order, which means largest
         * id first. That is to make sure we expand the property store file
         * only once.
         */
        for ( int i = propRecords.size() - 1; i >=0; i-- )
        {
            propStore.updateRecord( propRecords.get( i ) );
        }
        /*
         *  0 will always exist, if the map was empty we wouldn't be here
         *  and even one property will create at least one record.
         */
        return propRecords.get( 0 ).getId();
    }

    private void deletePropertyChain( long nextProp )
    {
        PropertyStore propStore = getPropertyStore();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propStore.getRecord( nextProp );
            /*
             *  The only reason to loop over the blocks is to handle the dynamic
             *  records that possibly hang under them. Otherwise, we could just
             *  set the property record not in use and be done with it. The
             *  residue of the convenience is that we do not remove individual
             *  property blocks - we just mark the whole record as !inUse.
             */
            for ( PropertyBlock propBlock : propRecord.getPropertyBlocks() )
            {
                if ( propBlock.isLight() )
                {
                    propStore.makeHeavy( propBlock );
                }
                for ( DynamicRecord rec : propBlock.getValueRecords() )
                {
                    rec.setInUse( false );
                    propRecord.addDeletedRecord( rec );
                }
            }
            propRecord.setInUse( false );
            nextProp = propRecord.getNextProp();
            propStore.updateRecord( propRecord );
        }
    }

    private Map<String, Object> getPropertyChain( long nextProp )
    {
        PropertyStore propStore = getPropertyStore();
        Map<String,Object> properties = new HashMap<String,Object>();

        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propStore.getRecord( nextProp );
            for ( PropertyBlock propBlock : propRecord.getPropertyBlocks() )
            {
                String key = indexHolder.getStringKey( propBlock.getKeyIndexId() );
                PropertyData propertyData = propBlock.newPropertyData( propRecord );
                Object value = propertyData.getValue() != null ? propertyData.getValue() :
                        propBlock.getType().getValue( propBlock, getPropertyStore() );
                properties.put( key, value );
            }
            nextProp = propRecord.getNextProp();
        }
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
        Collection<DynamicRecord> keyRecords =
            idxStore.allocateKeyRecords( keyBlockId, encodeString( stringKey ) );
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
        Collection<DynamicRecord> typeRecords =
            typeStore.allocateTypeNameRecords( typeBlockId, encodeString( name ) );
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
        String store = dir + fileSeparator + NeoStore.DEFAULT_NAME;
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
