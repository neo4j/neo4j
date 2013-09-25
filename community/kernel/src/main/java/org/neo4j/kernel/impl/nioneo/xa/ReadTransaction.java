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
package org.neo4j.kernel.impl.nioneo.xa;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.persistence.NeoStoreTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

import static org.neo4j.helpers.collection.IteratorUtil.asPrimitiveIterator;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;

class ReadTransaction implements NeoStoreTransaction
{
    private final NeoStore neoStore;

    public ReadTransaction( NeoStore neoStore )
    {
        this.neoStore = neoStore;
    }

    private NodeStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    private int getRelGrabSize()
    {
        return neoStore.getRelationshipGrabSize();
    }

    private RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    private PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    @Override
    public NodeRecord nodeLoadLight( long nodeId )
    {
        return getNodeStore().loadLightNode( nodeId );
    }

    @Override
    public RelationshipRecord relLoadLight( long id )
    {
        return getRelationshipStore().getLightRel( id );
    }

    @Override
    public long getRelationshipChainPosition( long nodeId )
    {
        return getNodeStore().getRecord( nodeId ).getNextRel();
    }

    @Override
    public Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> getMoreRelationships(
            long nodeId, long position )
    {
        return getMoreRelationships( nodeId, position, getRelGrabSize(), getRelationshipStore() );
    }

    static Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, Long> getMoreRelationships(
            long nodeId, long position, int grabSize, RelationshipStore relStore )
    {
        // initialCapacity=grabSize saves the lists the trouble of resizing
        List<RelationshipRecord> out = new ArrayList<>();
        List<RelationshipRecord> in = new ArrayList<>();
        List<RelationshipRecord> loop = null;
        Map<DirectionWrapper, Iterable<RelationshipRecord>> result = new EnumMap<>( DirectionWrapper.class );
        result.put( DirectionWrapper.OUTGOING, out );
        result.put( DirectionWrapper.INCOMING, in );
        for ( int i = 0; i < grabSize &&
            position != Record.NO_NEXT_RELATIONSHIP.intValue(); i++ )
        {
            RelationshipRecord relRecord = relStore.getChainRecord( position );
            if ( relRecord == null )
            {
                // return what we got so far
                return Pair.of( result, position );
            }
            long firstNode = relRecord.getFirstNode();
            long secondNode = relRecord.getSecondNode();
            if ( relRecord.inUse() )
            {
                if ( firstNode == secondNode )
                {
                    if ( loop == null )
                    {
                        // This is done lazily because loops are probably quite
                        // rarely encountered
                        loop = new ArrayList<>();
                        result.put( DirectionWrapper.BOTH, loop );
                    }
                    loop.add( relRecord );
                }
                else if ( firstNode == nodeId )
                {
                    out.add( relRecord );
                }
                else if ( secondNode == nodeId )
                {
                    in.add( relRecord );
                }
            }
            else
            {
                i--;
            }

            if ( firstNode == nodeId )
            {
                position = relRecord.getFirstNextRel();
            }
            else if ( secondNode == nodeId )
            {
                position = relRecord.getSecondNextRel();
            }
            else
            {
                throw new InvalidRecordException( "Node[" + nodeId +
                    "] is neither firstNode[" + firstNode +
                    "] nor secondNode[" + secondNode + "] for Relationship[" + relRecord.getId() + "]" );
            }
        }
        return Pair.of( result, position );
    }

    static void loadPropertyChain( Collection<PropertyRecord> chain, PropertyStore propertyStore,
            PropertyReceiver receiver )
    {
        if ( chain != null )
        {
            for ( PropertyRecord propRecord : chain )
            {
                for ( PropertyBlock propBlock : propRecord.getPropertyBlocks() )
                {
                    receiver.receive( propBlock.newPropertyData( propertyStore ), propRecord.getId() );
                }
            }
        }
    }

    static void loadProperties(
            PropertyStore propertyStore, long nextProp, PropertyReceiver receiver )
    {
        Collection<PropertyRecord> chain = propertyStore.getPropertyRecordChain( nextProp );
        if ( chain != null )
        {
            loadPropertyChain( chain, propertyStore, receiver );
        }
    }

    @Override
    public void relLoadProperties( long relId, boolean light, PropertyReceiver receiver )
    {
        RelationshipRecord relRecord = getRelationshipStore().getRecord( relId );
        if ( !relRecord.inUse() )
        {
            throw new InvalidRecordException( "Relationship[" + relId +
                "] not in use" );
        }
        loadProperties( getPropertyStore(), relRecord.getNextProp(), receiver );
    }

    @Override
    public void nodeLoadProperties( long nodeId, boolean light, PropertyReceiver receiver )
    {
        loadProperties( getPropertyStore(), getNodeStore().getRecord( nodeId ).getNextProp(), receiver );
    }

    @Override
    public void graphLoadProperties( boolean light, PropertyReceiver receiver )
    {
        loadProperties( getPropertyStore(), neoStore.getGraphNextProp(), receiver );
    }

    @Override
    public Token[] loadAllPropertyKeyTokens()
    {
        PropertyKeyTokenStore indexStore = getPropertyStore().getPropertyKeyTokenStore();
        return indexStore.getTokens( Integer.MAX_VALUE );
    }

    @Override
    public Token[] loadAllLabelTokens()
    {
        return neoStore.getLabelTokenStore().getTokens( Integer.MAX_VALUE );
    }

    @Override
    public void setXaConnection( XaConnection connection )
    {
    }

    @Override
    public boolean delistResource( Transaction tx, int tmsuccess )
        throws SystemException
    {
        throw readOnlyException();
    }

    private IllegalStateException readOnlyException()
    {
        return new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
    }

    @Override
    public void destroy()
    {
        throw readOnlyException();
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> nodeDelete( long nodeId )
    {
        throw readOnlyException();
    }

    @Override
    public DefinedProperty nodeAddProperty( long nodeId, int propertyKey, Object value )
    {
        throw readOnlyException();
    }

    @Override
    public DefinedProperty nodeChangeProperty( long nodeId, int propertyKey, Object value )
    {
        throw readOnlyException();
    }

    @Override
    public void nodeRemoveProperty( long nodeId, int propertyKey )
    {
        throw readOnlyException();
    }

    @Override
    public void nodeCreate( long id )
    {
        throw readOnlyException();
    }

    @Override
    public void relationshipCreate( long id, int typeId, long startNodeId, long endNodeId )
    {
        throw readOnlyException();
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> relDelete( long relId )
    {
        throw readOnlyException();
    }

    @Override
    public DefinedProperty relAddProperty( long relId, int propertyKey, Object value )
    {
        throw readOnlyException();
    }

    @Override
    public DefinedProperty relChangeProperty( long relId, int propertyKey, Object value )
    {
        throw readOnlyException();
    }

    @Override
    public void relRemoveProperty( long relId, int propertyKey )
    {
        throw readOnlyException();
    }

    @Override
    public Token[] loadRelationshipTypes()
    {
        Token relTypeData[] = neoStore.getRelationshipTypeStore().getTokens( Integer.MAX_VALUE );
        Token rawRelTypeData[] = new Token[relTypeData.length];
        for ( int i = 0; i < relTypeData.length; i++ )
        {
            rawRelTypeData[i] = new Token( relTypeData[i].name(), relTypeData[i].id() );
        }
        return rawRelTypeData;
    }

    @Override
    public void createPropertyKeyToken( String key, int id )
    {
        throw readOnlyException();
    }

    @Override
    public void createLabelToken( String name, int id )
    {
        throw readOnlyException();
    }

    @Override
    public void createRelationshipTypeToken( int id, String name )
    {
        throw readOnlyException();
    }

    @Override
    public DefinedProperty graphAddProperty( int propertyKey, Object value )
    {
        throw readOnlyException();
    }

    @Override
    public DefinedProperty graphChangeProperty( int propertyKey, Object value )
    {
        throw readOnlyException();
    }

    @Override
    public void graphRemoveProperty( int propertyKey )
    {
        throw readOnlyException();
    }

    @Override
    public void createSchemaRule( SchemaRule schemaRule )
    {
        throw readOnlyException();
    }

    @Override
    public void dropSchemaRule( SchemaRule rule )
    {
        throw readOnlyException();
    }

    @Override
    public void addLabelToNode( int labelId, long nodeId )
    {
        throw readOnlyException();
    }

    @Override
    public void removeLabelFromNode( int labelId, long nodeId )
    {
        throw readOnlyException();
    }

    @Override
    public PrimitiveLongIterator getLabelsForNode( long nodeId )
    {
        NodeRecord node = getNodeStore().getRecord( nodeId );
        return asPrimitiveIterator( parseLabelsField( node ).get( getNodeStore() ) );
    }

    @Override
    public void setConstraintIndexOwner( IndexRule constraintIndex, long constraintId )
    {
        throw readOnlyException();
    }
}
