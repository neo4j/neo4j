/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.transaction.Status;

import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.persistence.PersistenceManager.ResourceHolder;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArrayWithLoops;
import org.neo4j.kernel.impl.util.RelIdIterator;

import static org.neo4j.collection.primitive.Primitive.intObjectMap;

public class WritableTransactionState implements TransactionState
{
    // Dependencies
    private final Locks.Client locks;
    private final NodeManager nodeManager;
    private final RemoteTxHook txHook;
    private final TxIdGenerator txIdGenerator;

    // State
    private PrimitiveElement primitiveElement;
    private ResourceHolder neoStoreTransaction;

    private boolean isRemotelyInitialized = false;

    private static class MutableInteger
    {
        int value;
    }

    public static class SetAndDirectionCounter
    {
        final Collection<Long> set = new HashSet<>();
        final MutableInteger totalCount = new MutableInteger();
        private final Map<Direction, MutableInteger> counters = new EnumMap<>( Direction.class );
        {
            counters.put( Direction.OUTGOING, new MutableInteger() );
            counters.put( Direction.INCOMING, new MutableInteger() );
            counters.put( Direction.BOTH, new MutableInteger() );
        }

        void add( long id, Direction direction )
        {
            set.add( id );

            counters.get( direction ).value++;
            totalCount.value++;
        }

        int getCount( Direction direction )
        {
            return direction == Direction.BOTH ? totalCount.value :
                    counters.get( direction ).value + counters.get( Direction.BOTH ).value;
        }

        public int totalCount()
        {
            return totalCount.value;
        }
    }

    public static class PrimitiveElement
    {
        PrimitiveElement()
        {
        }

        private final ArrayMap<Long,CowNodeElement> nodes = new ArrayMap<>();
        private final ArrayMap<Long,CowRelElement> relationships = new ArrayMap<>();

        private final Set<Long> createdNodes = new HashSet<>();
        private final Set<Long> createdRelationships = new HashSet<>();

        private CowGraphElement graph;

        public CowNodeElement nodeElement( long id, boolean create )
        {
            CowNodeElement result = nodes.get( id );
            if ( result == null && create )
            {
                result = new CowNodeElement( id );
                nodes.put( id, result );
            }
            return result;
        }

        public CowRelElement relationshipElement( long id, boolean create )
        {
            CowRelElement result = relationships.get( id );
            if ( result == null && create )
            {
                result = new CowRelElement( id );
                relationships.put( id, result );
            }
            return result;
        }

        public CowGraphElement graphElement( boolean create )
        {
            if ( graph == null && create )
            {
                graph = new CowGraphElement();
            }
            return graph;
        }
    }

    static class CowEntityElement
    {
        protected long id;
        protected boolean deleted;
        protected ArrayMap<Integer, DefinedProperty> propertyAddMap;
        protected ArrayMap<Integer, DefinedProperty> propertyRemoveMap;

        CowEntityElement( long id )
        {
            this.id = id;
        }

        public long getId()
        {
            return id;
        }

        public ArrayMap<Integer, DefinedProperty> getPropertyAddMap( boolean create )
        {
            assertNotDeleted();
            if ( propertyAddMap == null && create )
            {
                propertyAddMap = new ArrayMap<>();
            }
            return propertyAddMap;
        }

        private void assertNotDeleted()
        {
            if ( isDeleted() )
            {
                throw new IllegalStateException( this + " has been deleted in this tx" );
            }
        }

        public boolean isDeleted()
        {
            return deleted;
        }

        public void setDeleted()
        {
            deleted = true;
        }

        public ArrayMap<Integer, DefinedProperty> getPropertyRemoveMap( boolean create )
        {
            if ( propertyRemoveMap == null && create )
            {
                propertyRemoveMap = new ArrayMap<>();
            }
            return propertyRemoveMap;
        }
    }

    public static class CowNodeElement extends CowEntityElement implements FirstRelationshipIds
    {
        CowNodeElement( long id )
        {
            super( id );
        }

        private long firstProp = Record.NO_NEXT_PROPERTY.intValue();
        private long firstRel = Record.NO_NEXT_RELATIONSHIP.intValue();

        private PrimitiveIntObjectMap<RelationshipGroupRecord> addedGroups;
        private ArrayMap<Integer, RelIdArray> relationshipAddMap;
        private ArrayMap<Integer, SetAndDirectionCounter> relationshipRemoveMap;
        public boolean dense;

        public ArrayMap<Integer, RelIdArray> getRelationshipAddMap( boolean create )
        {
            if ( relationshipAddMap == null && create )
            {
                relationshipAddMap = new ArrayMap<>();
            }
            return relationshipAddMap;
        }

        public RelIdArray getRelationshipAddMap( int type, boolean create )
        {
            ArrayMap<Integer, RelIdArray> map = getRelationshipAddMap( create );
            if ( map == null )
            {
                return null;
            }
            RelIdArray result = map.get( type );
            if ( result == null && create )
            {
                result = new RelIdArrayWithLoops( type );
                map.put( type, result );
            }
            return result;
        }

        public ArrayMap<Integer, SetAndDirectionCounter> getRelationshipRemoveMap( boolean create )
        {
            if ( relationshipRemoveMap == null && create )
            {
                relationshipRemoveMap = new ArrayMap<>();
            }
            return relationshipRemoveMap;
        }

        public SetAndDirectionCounter getRelationshipRemoveMap( int type, boolean create )
        {
            ArrayMap<Integer, SetAndDirectionCounter> map = getRelationshipRemoveMap( create );
            if ( map == null )
            {
                return null;
            }
            SetAndDirectionCounter result = map.get( type );
            if ( result == null && create )
            {
                result = new SetAndDirectionCounter();
                map.put( type, result );
            }
            return result;
        }

        public void addRelationshipGroupChange( RelationshipGroupRecord group )
        {
            if ( group.inUse() )
            {
                if ( addedGroups == null )
                {
                    addedGroups = intObjectMap();
                }
                addedGroups.put( group.getType(), group );
            }
        }

        @Override
        public long firstIdOf( int type, DirectionWrapper direction )
        {
            if ( !dense )
            {
                // For sparse nodes there's only a single relationship that is going to be the first
                // relationship in the chain, not a relationship per type and direction. So we'll return
                // that single relationship id, knowing that the caller of this method is also aware
                // of the sparse/dense node states and will use this id accordingly.
                return firstRel;
            }

            RelationshipGroupRecord group = addedGroups.get( type );
            if ( group == null )
            {
                return Record.NO_NEXT_RELATIONSHIP.intValue();
            }

            return direction.getNextRel( group );
        }

        @Override
        public String toString()
        {
            return "Node[" + id + "\n" +
                    "+" + relationshipAddMap + "\n" +
                    "-" + relationshipRemoveMap + "]";
        }
    }

    public static class CowRelElement extends CowEntityElement
    {
        CowRelElement( long id )
        {
            super( id );
        }

        @Override
        public String toString()
        {
            return "Relationship[" + id + "]";
        }
    }

    public static class CowGraphElement extends CowEntityElement
    {
        CowGraphElement()
        {
            super( -1 );
        }

        @Override
        public String toString()
        {
            return "Graph";
        }
    }

    public WritableTransactionState( Locks.Client locks,
                                     NodeManager nodeManager, RemoteTxHook txHook, TxIdGenerator txIdGenerator )
    {
        this.locks = locks;
        this.nodeManager = nodeManager;
        this.txHook = txHook;
        this.txIdGenerator = txIdGenerator;
    }

    @Override
    public Locks.Client locks()
    {
        return locks;
    }

    @Override
    public ArrayMap<Integer, SetAndDirectionCounter> getCowRelationshipRemoveMap( NodeImpl node )
    {
        if ( primitiveElement != null )
        {
            ArrayMap<Long, CowNodeElement> cowElements =
                    primitiveElement.nodes;
            CowNodeElement element = cowElements.get( node.getId() );
            if ( element != null )
            {
                return element.relationshipRemoveMap;
            }
        }
        return null;
    }

    @Override
    public SetAndDirectionCounter getOrCreateCowRelationshipRemoveMap( NodeImpl node, int type )
    {
        return getPrimitiveElement( true ).nodeElement( node.getId(), true ).getRelationshipRemoveMap( type, true );
    }

    @Override
    public void setFirstIds( long nodeId, boolean dense, long firstRel, long firstProp )
    {
        CowNodeElement nodeElement = getPrimitiveElement( true ).nodeElement( nodeId, true );
        nodeElement.firstProp = firstProp;
        nodeElement.firstRel = firstRel;
        nodeElement.dense = dense;
    }

    @Override
    public void addRelationshipGroupChange( long nodeId, RelationshipGroupRecord group )
    {
        CowNodeElement nodeElement = getPrimitiveElement( true ).nodeElement( nodeId, true );
        nodeElement.addRelationshipGroupChange( group );
    }

    @Override
    public ArrayMap<Integer, RelIdArray> getCowRelationshipAddMap( NodeImpl node )
    {
        PrimitiveElement primitiveElement = getPrimitiveElement( false );
        if ( primitiveElement == null )
        {
            return null;
        }
        CowNodeElement element = primitiveElement.nodeElement( node.getId(), false );
        return element != null ? element.relationshipAddMap : null;
    }

    @Override
    public RelIdArray getOrCreateCowRelationshipAddMap( NodeImpl node, int type )
    {
        return getPrimitiveElement( true ).nodeElement( node.getId(), true ).getRelationshipAddMap( type, true );
    }

    @Override
    public void commit()
    {
        locks.close();
    }

    @Override
    public void commitCows( CacheAccessBackDoor cacheAccess )
    {
        applyTransactionStateToCache( Status.STATUS_COMMITTED, cacheAccess );
    }

    @Override
    public void rollback()
    {
        try
        {
            applyTransactionStateToCache( Status.STATUS_ROLLEDBACK, null );
        }
        finally
        {
            locks.close();
        }
    }

    private void applyTransactionStateToCache( int param, CacheAccessBackDoor cacheAccess )
    {
        if ( primitiveElement == null )
        {
            return;
        }
        ArrayMap<Long, CowNodeElement> cowNodeElements = primitiveElement.nodes;
        Set<Entry<Long, CowNodeElement>> nodeEntrySet =
                cowNodeElements.entrySet();
        for ( Entry<Long, CowNodeElement> entry : nodeEntrySet )
        {
            NodeImpl node = nodeManager.getNodeIfCached( entry.getKey() );
            if ( node != null )
            {
                CowNodeElement nodeElement = entry.getValue();
                if ( param == Status.STATUS_COMMITTED )
                {
                    if ( node.commitRelationshipMaps(
                            nodeElement.relationshipAddMap,
                            nodeElement.relationshipRemoveMap,
                            nodeElement,
                            nodeElement.dense ) )
                    {
                        cacheAccess.removeNodeFromCache( node.getId() );
                    }
                    node.commitPropertyMaps( nodeElement.propertyAddMap,
                            nodeElement.propertyRemoveMap, nodeElement.firstProp );
                }
                else if ( param != Status.STATUS_ROLLEDBACK )
                {
                    throw new TransactionFailureException(
                            "Unknown transaction status: " + param );
                }
                int sizeAfter = node.sizeOfObjectInBytesIncludingOverhead();
                nodeManager.updateCacheSize( node, sizeAfter );
            }
        }
        ArrayMap<Long, CowRelElement> cowRelElements = primitiveElement.relationships;
        Set<Entry<Long, CowRelElement>> relEntrySet =
                cowRelElements.entrySet();
        for ( Entry<Long, CowRelElement> entry : relEntrySet )
        {
            RelationshipImpl rel = nodeManager.getRelIfCached( entry.getKey() );
            if ( rel != null )
            {
                CowRelElement relElement = entry.getValue();
                if ( param == Status.STATUS_COMMITTED )
                {
                    rel.commitPropertyMaps( relElement.propertyAddMap,
                            relElement.propertyRemoveMap, Record.NO_NEXT_PROPERTY.intValue() );
                }
                else if ( param != Status.STATUS_ROLLEDBACK )
                {
                    throw new TransactionFailureException(
                            "Unknown transaction status: " + param );
                }
                int sizeAfter = rel.sizeOfObjectInBytesIncludingOverhead();
                nodeManager.updateCacheSize( rel, sizeAfter );
            }
        }
        if ( primitiveElement.graph != null && param == Status.STATUS_COMMITTED )
        {
            nodeManager.getGraphProperties().commitPropertyMaps( primitiveElement.graph.getPropertyAddMap( false ),
                    primitiveElement.graph.getPropertyRemoveMap( false ), Record.NO_NEXT_PROPERTY.intValue() );
        }
    }

    @Override
    public ArrayMap<Integer,DefinedProperty> getCowPropertyRemoveMap( Primitive primitive )
    {
        if ( primitiveElement == null )
        {
            return null;
        }
        CowEntityElement element = primitive.getEntityElement( primitiveElement, false );
        return element != null ? element.getPropertyRemoveMap( false ) : null;
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> getCowPropertyAddMap( Primitive primitive )
    {
        if ( primitiveElement == null )
        {
            return null;
        }
        CowEntityElement element = primitive.getEntityElement( primitiveElement, false );
        return element != null ? element.getPropertyAddMap( false ) : null;
    }

    private PrimitiveElement getPrimitiveElement( boolean create )
    {
        if ( primitiveElement == null && create )
        {
            primitiveElement = new PrimitiveElement();
        }
        return primitiveElement;
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> getOrCreateCowPropertyAddMap( Primitive primitive )
    {
        return primitive.getEntityElement( getPrimitiveElement( true ), true ).getPropertyAddMap( true );
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> getOrCreateCowPropertyRemoveMap( Primitive primitive )
    {
        return primitive.getEntityElement( getPrimitiveElement( true ), true ).getPropertyRemoveMap( true );
    }

    @Override
    public void createNode( long id )
    {
        getPrimitiveElement( true ).createdNodes.add( id );
    }

    @Override
    public void createRelationship( long id )
    {
        getPrimitiveElement( true ).createdRelationships.add( id );
    }

    @Override
    public void deleteNode( long id )
    {
        PrimitiveElement element = getPrimitiveElement( true );
        element.nodeElement( id, true ).setDeleted();
    }

    @Override
    public void deleteRelationship( long id )
    {
        PrimitiveElement element = getPrimitiveElement( true );
        element.relationshipElement( id, true ).setDeleted();
    }

    @Override
    public TransactionData getTransactionData()
    {
        TransactionDataImpl result = new TransactionDataImpl();
        populateCreatedNodes( primitiveElement, result );
        if ( primitiveElement == null )
        {
            return result;
        }
        populateNodeRelEvent( primitiveElement, result );
        populateRelationshipPropertyEvents( primitiveElement, result );
        return result;
    }

    private void populateRelationshipPropertyEvents( PrimitiveElement element, TransactionDataImpl result )
    {
        for ( long relId : element.relationships.keySet() )
        {
            CowRelElement relElement = element.relationships.get( relId );
            RelationshipProxy rel = nodeManager.newRelationshipProxyById( relId );
            RelationshipImpl relImpl = nodeManager.getRelationshipForProxy( relId );
            if ( relElement.isDeleted() )
            {
                if ( primitiveElement.createdRelationships.contains( relId ) )
                {
                    continue;
                }
                // note: this is done in node populate data
                // result.deleted( rel );
            }
            if ( relElement.propertyAddMap != null && !relElement.isDeleted() )
            {
                for ( DefinedProperty data : relElement.propertyAddMap.values() )
                {
                    String key = nodeManager.getKeyForProperty( data );
                    Object oldValue = relImpl.getCommittedPropertyValue( nodeManager, key );
                    Object newValue = data.value();
                    result.assignedProperty( rel, key, newValue, oldValue );
                }
            }
            if ( relElement.propertyRemoveMap != null )
            {
                for ( DefinedProperty data : relElement.propertyRemoveMap.values() )
                {
                    String key = nodeManager.getKeyForProperty( data );
                    Object oldValue = data.value();
                    if ( oldValue != null && !relElement.isDeleted() )
                    {
                        relImpl.getCommittedPropertyValue( nodeManager, key );
                    }
                    result.removedProperty( rel, key, oldValue );
                }
            }
        }
    }

    private void populateNodeRelEvent( PrimitiveElement element, TransactionDataImpl result )
    {
        for ( long nodeId : element.nodes.keySet() )
        {
            CowNodeElement nodeElement = element.nodes.get( nodeId );
            NodeProxy node = nodeManager.newNodeProxyById( nodeId );
            NodeImpl nodeImpl = nodeManager.getNodeForProxy( nodeId );
            if ( nodeElement.isDeleted() )
            {
                if ( primitiveElement.createdNodes.contains( nodeId ) )
                {
                    continue;
                }
                result.deleted( node );
            }
            if ( nodeElement.relationshipAddMap != null && !nodeElement.isDeleted() )
            {
                for ( Integer type : nodeElement.relationshipAddMap.keySet() )
                {
                    RelIdArray createdRels = nodeElement.relationshipAddMap.get( type );
                    populateNodeRelEvent( element, result, nodeId, createdRels );
                }
            }
            if ( nodeElement.relationshipRemoveMap != null )
            {
                for ( Integer type : nodeElement.relationshipRemoveMap.keySet() )
                {
                    SetAndDirectionCounter deletedRels = nodeElement.relationshipRemoveMap.get( type );
                    for ( long relId : deletedRels.set )
                    {
                        if ( primitiveElement.createdRelationships.contains( relId ) )
                        {
                            continue;
                        }
                        RelationshipProxy rel = nodeManager.newRelationshipProxyById( relId );
                        if ( rel.getStartNode().getId() == nodeId )
                        {
                            result.deleted( nodeManager.newRelationshipProxyById( relId ) );
                        }
                    }
                }
            }
            if ( nodeElement.propertyAddMap != null && !nodeElement.isDeleted() )
            {
                for ( DefinedProperty data : nodeElement.propertyAddMap.values() )
                {
                    String key = nodeManager.getKeyForProperty( data );
                    Object oldValue = nodeImpl.getCommittedPropertyValue( nodeManager, key );
                    Object newValue = data.value();
                    result.assignedProperty( node, key, newValue, oldValue );
                }
            }
            if ( nodeElement.propertyRemoveMap != null )
            {
                for ( DefinedProperty data : nodeElement.propertyRemoveMap.values() )
                {
                    String key = nodeManager.getKeyForProperty( data );
                    Object oldValue = data.value();
                    if ( oldValue == null && !nodeElement.isDeleted() )
                    {
                        nodeImpl.getCommittedPropertyValue( nodeManager, key );
                    }
                    result.removedProperty( node, key, oldValue );
                }
            }
        }
    }

    private void populateNodeRelEvent( PrimitiveElement element, TransactionDataImpl result,
                                       long nodeId, RelIdArray createdRels )
    {
        for ( RelIdIterator iterator = createdRels.iterator( DirectionWrapper.BOTH ); iterator.hasNext(); )
        {
            long relId = iterator.next();
            CowRelElement relElement = element.relationships.get( relId );
            if ( relElement != null && relElement.isDeleted() )
            {
                continue;
            }
            RelationshipProxy rel = nodeManager.newRelationshipProxyById( relId );
            if ( rel.getStartNode().getId() == nodeId )
            {
                result.created( nodeManager.newRelationshipProxyById( relId ) );
            }
        }
    }

    private void populateCreatedNodes( PrimitiveElement element,
                                       TransactionDataImpl result )
    {
        for ( Long nodeId : getCreatedNodes() )
        {
            if ( element != null )
            {
                CowNodeElement nodeElement = element.nodes.get( nodeId );
                if ( nodeElement != null && nodeElement.isDeleted() )
                {
                    continue;
                }
            }
            result.created( nodeManager.newNodeProxyById( nodeId ) );
        }
    }

    @Override
    public Set<Long> getCreatedNodes()
    {
        return primitiveElement != null ? primitiveElement.createdNodes : Collections.<Long>emptySet();
    }

    @Override
    public Set<Long> getCreatedRelationships()
    {
        return primitiveElement != null ? primitiveElement.createdRelationships : Collections.<Long>emptySet();
    }

    @Override
    public Iterable<CowNodeElement> getChangedNodes()
    {
        if ( primitiveElement == null )
        {
            return Iterables.empty();
        }

        return primitiveElement.nodes.values();
    }

    @Override
    public boolean nodeIsDeleted( long nodeId )
    {
        if ( primitiveElement == null )
        {
            return false;
        }

        CowNodeElement nodeElement = primitiveElement.nodeElement( nodeId, false );
        return nodeElement != null && nodeElement.isDeleted();
    }

    @Override
    public boolean relationshipIsDeleted( long relationshipId )
    {
        if ( primitiveElement == null )
        {
            return false;
        }
        CowRelElement relationshipElement = primitiveElement.relationshipElement( relationshipId, false );
        return relationshipElement != null && relationshipElement.isDeleted();
    }

    @Override
    public boolean hasChanges()
    {
        return primitiveElement != null;
    }

    @Override
    public RemoteTxHook getTxHook()
    {
        return txHook;
    }

    @Override
    public TxIdGenerator getTxIdGenerator()
    {
        return txIdGenerator;
    }

    @Override
    public boolean isRemotelyInitialized()
    {
        return isRemotelyInitialized;
    }

    @Override
    public void markAsRemotelyInitialized()
    {
        isRemotelyInitialized = true;
    }

    @Override
    public ResourceHolder getNeoStoreTransaction()
    {
        return neoStoreTransaction;
    }

    @Override
    public void setNeoStoreTransaction( ResourceHolder neoStoreTransaction )
    {
        this.neoStoreTransaction = neoStoreTransaction;
    }
}
