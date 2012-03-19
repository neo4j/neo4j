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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArrayWithLoops;
import org.neo4j.kernel.impl.util.RelIdIterator;

/**
 * Manages object version diffs and locks for each transaction.
 */
public class LockReleaser
{
    private static Logger log = Logger.getLogger( LockReleaser.class.getName() );

    private final ArrayMap<Transaction,List<LockElement>> lockMap =
        new ArrayMap<Transaction,List<LockElement>>( (byte)5, true, true );
    private final ArrayMap<Transaction,PrimitiveElement> cowMap =
        new ArrayMap<Transaction,PrimitiveElement>( (byte)5, true, true );

    private NodeManager nodeManager;
    private final LockManager lockManager;
    private final TransactionManager transactionManager;
    private PropertyIndexManager propertyIndexManager;

    public void setNodeManager(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    public static class PrimitiveElement
    {
        PrimitiveElement() {}

        private final ArrayMap<Long,CowNodeElement> nodes =
            new ArrayMap<Long,CowNodeElement>();
        private final ArrayMap<Long,CowRelElement> relationships =
            new ArrayMap<Long,CowRelElement>();
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
            if ( graph == null && create ) graph = new CowGraphElement();
            return graph;
        }
    }

    static class CowEntityElement
    {
        protected long id;
        protected boolean deleted;
        protected ArrayMap<Integer,PropertyData> propertyAddMap;
        protected ArrayMap<Integer,PropertyData> propertyRemoveMap;

        CowEntityElement( long id )
        {
            this.id = id;
        }

        public ArrayMap<Integer, PropertyData> getPropertyAddMap( boolean create )
        {
            assertNotDeleted();
            if ( propertyAddMap == null && create ) propertyAddMap = new ArrayMap<Integer, PropertyData>();
            return propertyAddMap;
        }

        private void assertNotDeleted()
        {
            if ( deleted ) throw new IllegalStateException( this + " has been deleted in this tx" );
        }

        public ArrayMap<Integer, PropertyData> getPropertyRemoveMap( boolean create )
        {
            if ( propertyRemoveMap == null && create ) propertyRemoveMap = new ArrayMap<Integer, PropertyData>();
            return propertyRemoveMap;
        }
    }

    public static class CowNodeElement extends CowEntityElement
    {
        CowNodeElement( long id )
        {
            super( id );
        }

        private long firstRel = Record.NO_NEXT_RELATIONSHIP.intValue();
        private long firstProp = Record.NO_NEXT_PROPERTY.intValue();

        private ArrayMap<String,RelIdArray> relationshipAddMap;
        private ArrayMap<String,Collection<Long>> relationshipRemoveMap;

        public ArrayMap<String, RelIdArray> getRelationshipAddMap( boolean create )
        {
            if ( relationshipAddMap == null && create ) relationshipAddMap = new ArrayMap<String, RelIdArray>();
            return relationshipAddMap;
        }

        public RelIdArray getRelationshipAddMap( String type, boolean create )
        {
            ArrayMap<String, RelIdArray> map = getRelationshipAddMap( create );
            if ( map == null ) return null;
            RelIdArray result = map.get( type );
            if ( result == null && create )
            {
                result = new RelIdArrayWithLoops( type );
                map.put( type, result );
            }
            return result;
        }

        public ArrayMap<String, Collection<Long>> getRelationshipRemoveMap( boolean create )
        {
            if ( relationshipRemoveMap == null && create ) relationshipRemoveMap = new ArrayMap<String, Collection<Long>>();
            return relationshipRemoveMap;
        }

        public Collection<Long> getRelationshipRemoveMap( String type, boolean create )
        {
            ArrayMap<String, Collection<Long>> map = getRelationshipRemoveMap( create );
            if ( map == null ) return null;
            Collection<Long> result = map.get( type );
            if ( result == null && create )
            {
                result = new HashSet<Long>();
                map.put( type, result );
            }
            return result;
        }

        @Override
        public String toString()
        {
            return "Node[" + id + "]";
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

    public LockReleaser( LockManager lockManager,
        TransactionManager transactionManager,
        NodeManager nodeManager,
        PropertyIndexManager propertyIndexManager)
    {
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.nodeManager = nodeManager;
        this.propertyIndexManager = propertyIndexManager;
    }

    public static class LockElement
    {
        private final Object resource;
        private final LockType lockType;
        private boolean released;

        LockElement( Object resource, LockType type )
        {
            this.resource = resource;
            this.lockType = type;
        }

        public boolean releaseIfAcquired( LockManager lockManager )
        {
            if ( released ) return false;
            lockType.release( resource, lockManager );
            return (released = true);
        }

        @Override
        public String toString()
        {
            StringBuilder string = new StringBuilder( lockType.name() ).append( "-LockElement[" );
            if ( released ) string.append( "released," );
            string.append( resource );
            return string.append( ']' ).toString();
        }
    }

    /**
     * Invoking this method with no transaction running will cause the lock to
     * be released right away.
     *
     * @param resource
     *            the resource on which the lock is taken
     * @param type
     *            type of lock (READ or WRITE)
     * @throws NotInTransactionException
     */
    public LockElement addLockToTransaction( Object resource, LockType type )
        throws NotInTransactionException
    {
        Transaction tx = getTransaction();
        List<LockElement> lockElements = lockMap.get( tx );
        if ( lockElements != null )
        {
            LockElement element = new LockElement( resource, type );
            lockElements.add( element );
            return element;
        }
        else
        {
            if ( tx == null )
            {
                // no transaction we release lock right away
                type.release( resource, lockManager );
                return null;
            }
            lockElements = new ArrayList<LockElement>();
            lockMap.put( tx, lockElements );
            LockElement element = new LockElement( resource, type );
            lockElements.add( element );
            // we have to have a synchronization hook for read only transaction,
            // write locks can be taken in read only transactions (ex:
            // transactions that perform write operations that cancel each other
            // out). This sync hook will only release locks if they exist and
            // tx was read only
            try
            {
                tx.registerSynchronization( new ReadOnlyTxReleaser( tx ) );
            }
            catch ( Exception e )
            {
                throw new TransactionFailureException(
                    "Failed to register lock release synchronization hook", e );
            }
            return element;
        }
    }

    private Transaction getTransaction()
    {
        try
        {
            return transactionManager.getTransaction();
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException(
                "Failed to get current transaction.", e );
        }
    }

    public Collection<Long> getCowRelationshipRemoveMap( NodeImpl node, String type )
    {
        PrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement != null )
        {
            ArrayMap<Long,CowNodeElement> cowElements =
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( node.getId() );
            if ( element != null && element.relationshipRemoveMap != null )
            {
                return element.relationshipRemoveMap.get( type );
            }
        }
        return null;
    }

    public Collection<Long> getOrCreateCowRelationshipRemoveMap( NodeImpl node, String type )
    {
        return getPrimitiveElement( true ).nodeElement( node.getId(), true ).getRelationshipRemoveMap( type, true );
    }

    public void setFirstIds( long nodeId, long firstRel, long firstProp )
    {
        CowNodeElement nodeElement = getPrimitiveElement( true ).nodeElement( nodeId, true );
        nodeElement.firstRel = firstRel;
        nodeElement.firstProp = firstProp;
    }

    public ArrayMap<String,RelIdArray> getCowRelationshipAddMap( NodeImpl node )
    {
        PrimitiveElement primitiveElement = getPrimitiveElement( false );
        if ( primitiveElement == null ) return null;
        CowNodeElement element = primitiveElement.nodeElement( node.getId(), false );
        return element != null ? element.relationshipAddMap : null;
    }

    public RelIdArray getCowRelationshipAddMap( NodeImpl node, String type )
    {
        ArrayMap<String, RelIdArray> map = getCowRelationshipAddMap( node );
        return map != null ? map.get( type ) : null;
    }

    public RelIdArray getOrCreateCowRelationshipAddMap( NodeImpl node, String type )
    {
        return getPrimitiveElement( true ).nodeElement( node.getId(), true ).getRelationshipAddMap( type, true );
    }

    public void commit()
    {
        Transaction tx = getTransaction();
        // propertyIndex
        releaseLocks( tx );
    }

    public void commitCows()
    {
        Transaction tx = getTransaction();
        propertyIndexManager.commit( tx );
        releaseCows( tx, Status.STATUS_COMMITTED );
    }

    public void rollback()
    {
        Transaction tx = getTransaction();
        // propertyIndex
        propertyIndexManager.rollback( tx );
        releaseCows( tx, Status.STATUS_ROLLEDBACK );
        releaseLocks( tx );
    }

    public boolean hasLocks( Transaction tx )
    {
        List<LockElement> lockElements = lockMap.get( tx );
        return lockElements != null && !lockElements.isEmpty();
    }

    void releaseLocks( Transaction tx )
    {
        List<LockElement> lockElements = lockMap.remove( tx );
        if ( lockElements != null )
        {
            for ( LockElement lockElement : lockElements )
            {
                try
                {
                    lockElement.releaseIfAcquired( lockManager );
                }
                catch ( Exception e )
                {
                    log.log( Level.SEVERE, "Unable to release lock[" + lockElement.lockType + "] on resource["
                                           + lockElement.resource + "]", e );
                }
            }
        }
    }

    void releaseCows( Transaction cowTxId, int param )
    {
        PrimitiveElement element = cowMap.remove( cowTxId );
        if ( element == null )
        {
            return;
        }
        ArrayMap<Long,CowNodeElement> cowNodeElements = element.nodes;
        Set<Entry<Long,CowNodeElement>> nodeEntrySet =
            cowNodeElements.entrySet();
        for ( Entry<Long,CowNodeElement> entry : nodeEntrySet )
        {
            NodeImpl node = nodeManager.getNodeIfCached( entry.getKey() );
            if ( node != null )
            {
                int sizeBefore = node.size();
                CowNodeElement nodeElement = entry.getValue();
                if ( param == Status.STATUS_COMMITTED )
                {
                    node.commitRelationshipMaps( nodeElement.relationshipAddMap,
                        nodeElement.relationshipRemoveMap, nodeElement.firstRel );
                    node.commitPropertyMaps( nodeElement.propertyAddMap,
                        nodeElement.propertyRemoveMap, nodeElement.firstProp );
                }
                else if ( param != Status.STATUS_ROLLEDBACK )
                {
                    throw new TransactionFailureException(
                        "Unknown transaction status: " + param );
                }
                int sizeAfter = node.size();
                nodeManager.updateCacheSize( node, sizeBefore, sizeAfter );
            }
        }
        ArrayMap<Long,CowRelElement> cowRelElements = element.relationships;
        Set<Entry<Long,CowRelElement>> relEntrySet =
            cowRelElements.entrySet();
        for ( Entry<Long,CowRelElement> entry : relEntrySet )
        {
            RelationshipImpl rel = nodeManager.getRelIfCached( entry.getKey() );
            if ( rel != null )
            {
                int sizeBefore = rel.size();
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
                int sizeAfter = rel.size();
                nodeManager.updateCacheSize( rel, sizeBefore, sizeAfter );
            }
        }
        if ( element.graph != null && param == Status.STATUS_COMMITTED )
        {
            nodeManager.getGraphProperties().commitPropertyMaps( element.graph.getPropertyAddMap( false ),
                    element.graph.getPropertyRemoveMap( false ), Record.NO_NEXT_PROPERTY.intValue() );
        }
        cowMap.remove( cowTxId );
    }

    // non thread safe but let exception be thrown instead of risking deadlock
    public void dumpLocks()
    {
        System.out.print( "Locks held: " );
        java.util.Iterator<?> itr = lockMap.keySet().iterator();
        if ( !itr.hasNext() )
        {
            System.out.println( "NONE" );
        }
        else
        {
            System.out.println();
        }
        while ( itr.hasNext() )
        {
            Transaction transaction = (Transaction) itr.next();
            System.out.println( "" + transaction + "->" +
                lockMap.get( transaction ).size() );
        }
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyRemoveMap(
        Primitive primitive )
    {
        PrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement == null ) return null;
        CowEntityElement element = primitive.getEntityElement( primitiveElement, false );
        return element != null ? element.getPropertyRemoveMap( false ) : null;
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyAddMap(
        Primitive primitive )
    {
        PrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement == null ) return null;
        CowEntityElement element = primitive.getEntityElement( primitiveElement, false );
        return element != null ? element.getPropertyAddMap( false ) : null;
    }

    public PrimitiveElement getPrimitiveElement( boolean create )
    {
        return getPrimitiveElement( getTransaction(), create );
    }

    public PrimitiveElement getPrimitiveElement( Transaction tx, boolean create )
    {
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        PrimitiveElement primitiveElement = cowMap.get( tx );
        if ( primitiveElement == null && create )
        {
            primitiveElement = new PrimitiveElement();
            cowMap.put( tx, primitiveElement );
        }
        return primitiveElement;
    }

    public ArrayMap<Integer,PropertyData> getOrCreateCowPropertyAddMap(
        Primitive primitive )
    {
        return primitive.getEntityElement( getPrimitiveElement( true ), true ).getPropertyAddMap( true );
    }

    public ArrayMap<Integer,PropertyData> getOrCreateCowPropertyRemoveMap(
        Primitive primitive )
    {
        return primitive.getEntityElement( getPrimitiveElement( true ), true ).getPropertyRemoveMap( true );
    }

    public void deletePrimitive( Primitive primitive )
    {
        primitive.getEntityElement( getPrimitiveElement( true ), true ).deleted = true;
    }

    public void removeNodeFromCache( long nodeId )
    {
        if ( nodeManager != null )
        {
            nodeManager.removeNodeFromCache( nodeId );
        }
    }

    public void addRelationshipType( NameData type )
    {
        if ( nodeManager != null )
        {
            nodeManager.addRelationshipType( type );
        }
    }

    public void addPropertyIndex( NameData index )
    {
        if ( nodeManager != null )
        {
            nodeManager.addPropertyIndex( index );
        }
    }

    public void removeRelationshipFromCache( long id )
    {
        if ( nodeManager != null )
        {
            nodeManager.removeRelationshipFromCache( id );
        }
    }

    public void removeRelationshipTypeFromCache( int id )
    {
        if ( nodeManager != null )
        {
            nodeManager.removeRelationshipTypeFromCache( id );
        }
    }

    public void removeGraphPropertiesFromCache()
    {
        if ( nodeManager != null )
        {
            nodeManager.removeGraphPropertiesFromCache();
        }
    }

    private class ReadOnlyTxReleaser implements Synchronization
    {
        private final Transaction tx;

        ReadOnlyTxReleaser( Transaction tx )
        {
            this.tx = tx;
        }

        public void afterCompletion( int status )
        {
            releaseLocks( tx );
        }

        public void beforeCompletion()
        {
        }
    }

    public void clearCache()
    {
        if ( nodeManager != null )
        {
            nodeManager.clearCache();
        }
    }

    public TransactionData getTransactionData()
    {
        TransactionDataImpl result = new TransactionDataImpl();
        PrimitiveElement element = cowMap.get( getTransaction() );
        populateCreatedNodes( element, result );
        if ( element == null )
        {
            return result;
        }
        if ( element.nodes != null )
        {
            populateNodeRelEvent( element, result );
        }
        if ( element.relationships != null )
        {
            populateRelationshipPropertyEvents( element, result );
        }
        return result;
    }

    private void populateRelationshipPropertyEvents( PrimitiveElement element,
            TransactionDataImpl result )
    {
        for ( long relId : element.relationships.keySet() )
        {
            CowRelElement relElement = element.relationships.get( relId );
            RelationshipProxy rel = nodeManager.newRelationshipProxyById( relId );
            RelationshipImpl relImpl = nodeManager.getRelationshipForProxy( relId, null );
            if ( relElement.deleted )
            {
                if ( nodeManager.relCreated( relId ) )
                {
                    continue;
                }
                // note: this is done in node populate data
                // result.deleted( rel );
            }
            if ( relElement.propertyAddMap != null && !relElement.deleted )
            {
                for ( PropertyData data : relElement.propertyAddMap.values() )
                {
                    String key = nodeManager.getKeyForProperty( data );
                    Object oldValue = relImpl.getCommittedPropertyValue( nodeManager, key );
                    Object newValue = data.getValue();
                    result.assignedProperty( rel, key, newValue, oldValue );
                }
            }
            if ( relElement.propertyRemoveMap != null )
            {
                for ( PropertyData data : relElement.propertyRemoveMap.values() )
                {
                    String key = nodeManager.getKeyForProperty( data );
                    Object oldValue = data.getValue();
                    if ( oldValue != null && !relElement.deleted )
                    {
                        relImpl.getCommittedPropertyValue( nodeManager, key );
                    }
                    result.removedProperty( rel, key, oldValue );
                }
            }
        }
    }

    private void populateNodeRelEvent( PrimitiveElement element,
            TransactionDataImpl result )
    {
        for ( long nodeId : element.nodes.keySet() )
        {
            CowNodeElement nodeElement = element.nodes.get( nodeId );
            NodeProxy node = nodeManager.newNodeProxyById( nodeId );
            NodeImpl nodeImpl = nodeManager.getNodeForProxy( nodeId, null );
            if ( nodeElement.deleted )
            {
                if ( nodeManager.nodeCreated( nodeId ) )
                {
                    continue;
                }
                result.deleted( node );
            }
            if ( nodeElement.relationshipAddMap != null && !nodeElement.deleted )
            {
                for ( String type : nodeElement.relationshipAddMap.keySet() )
                {
                    RelIdArray createdRels = nodeElement.relationshipAddMap.get( type );
                    populateNodeRelEvent( element, result, nodeId, createdRels );
                }
            }
            if ( nodeElement.relationshipRemoveMap != null )
            {
                for ( String type : nodeElement.relationshipRemoveMap.keySet() )
                {
                    Collection<Long> deletedRels = nodeElement.relationshipRemoveMap.get( type );
                    for ( long relId : deletedRels )
                    {
                        if ( nodeManager.relCreated( relId ) )
                        {
                            continue;
                        }
                        RelationshipProxy rel = nodeManager.newRelationshipProxyById( relId );
                        if ( rel.getStartNode().getId() == nodeId )
                        {
                            result.deleted( nodeManager.newRelationshipProxyById( relId ));
                        }
                    }
                }
            }
            if ( nodeElement.propertyAddMap != null && !nodeElement.deleted )
            {
                for ( PropertyData data : nodeElement.propertyAddMap.values() )
                {
                    String key = nodeManager.getKeyForProperty( data );
                    Object oldValue = nodeImpl.getCommittedPropertyValue( nodeManager, key );
                    Object newValue = data.getValue();
                    result.assignedProperty( node, key, newValue, oldValue );
                }
            }
            if ( nodeElement.propertyRemoveMap != null )
            {
                for ( PropertyData data : nodeElement.propertyRemoveMap.values() )
                {
                    String key = nodeManager.getKeyForProperty( data );
                    Object oldValue = data.getValue();
                    if ( oldValue == null && !nodeElement.deleted )
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
            if ( relElement != null && relElement.deleted )
            {
                continue;
            }
            RelationshipProxy rel = nodeManager.newRelationshipProxyById( relId );
            if ( rel.getStartNode().getId() == nodeId )
            {
                result.created( nodeManager.newRelationshipProxyById( relId ));
            }
        }
    }

    private void populateCreatedNodes( PrimitiveElement element,
            TransactionDataImpl result )
    {
        RelIdArray createdNodes = nodeManager.getCreatedNodes();
        for ( RelIdIterator iterator = createdNodes.iterator( DirectionWrapper.BOTH ); iterator.hasNext(); )
        {
            long nodeId = iterator.next();
            if ( element != null && element.nodes != null )
            {
                CowNodeElement nodeElement = element.nodes.get( nodeId );
                if ( nodeElement != null && nodeElement.deleted )
                {
                    continue;
                }
            }
            result.created( nodeManager.newNodeProxyById( nodeId ) );
        }
    }

    boolean hasRelationshipModifications( NodeImpl node )
    {
        Transaction tx = getTransaction();
        if ( tx == null )
        {
            return false;
        }
        PrimitiveElement primitiveElement = cowMap.get( tx );
        if ( primitiveElement != null )
        {
            ArrayMap<Long,CowNodeElement> cowElements =
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( node.getId() );
            if ( element != null && (element.relationshipAddMap != null || element.relationshipRemoveMap != null) )
            {
                return true;
            }
        }
        return false;
    }
}