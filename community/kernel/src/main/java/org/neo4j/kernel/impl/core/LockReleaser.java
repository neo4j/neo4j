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
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexData;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeData;
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

    private static class PrimitiveElement
    {
        PrimitiveElement()
        {
        }

        final ArrayMap<Long,CowNodeElement> nodes =
            new ArrayMap<Long,CowNodeElement>();
        final ArrayMap<Long,CowRelElement> relationships =
            new ArrayMap<Long,CowRelElement>();
    }

    private static class CowNodeElement
    {
        CowNodeElement()
        {

        }

        boolean deleted = false;

        ArrayMap<String,RelIdArray> relationshipAddMap = null;
        ArrayMap<String,Collection<Long>> relationshipRemoveMap = null;
        ArrayMap<Integer,PropertyData> propertyAddMap = null;
        ArrayMap<Integer,PropertyData> propertyRemoveMap = null;
    }

    private static class CowRelElement
    {
        CowRelElement()
        {

        }

        boolean deleted = false;

        ArrayMap<Integer,PropertyData> propertyAddMap = null;
        ArrayMap<Integer,PropertyData> propertyRemoveMap = null;
    }

    public LockReleaser( LockManager lockManager,
        TransactionManager transactionManager )
    {
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
    }

    void setNodeManager( NodeManager nodeManager )
    {
        this.nodeManager = nodeManager;
    }

    void setPropertyIndexManager( PropertyIndexManager propertyIndexManager )
    {
        this.propertyIndexManager = propertyIndexManager;
    }

    private static class LockElement
    {
        Object resource;
        LockType lockType;

        LockElement( Object resource, LockType type )
        {
            this.resource = resource;
            this.lockType = type;
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
    public void addLockToTransaction( Object resource, LockType type )
        throws NotInTransactionException
    {
        Transaction tx = getTransaction();
        List<LockElement> lockElements = lockMap.get( tx );
        if ( lockElements != null )
        {
            lockElements.add( new LockElement( resource, type ) );
        }
        else
        {
            if ( tx == null )
            {
                // no transaction we release lock right away
                if ( type == LockType.WRITE )
                {
                    lockManager.releaseWriteLock( resource, null );
                }
                else if ( type == LockType.READ )
                {
                    lockManager.releaseReadLock( resource, null );
                }
                return;
            }
            lockElements = new ArrayList<LockElement>();
            lockMap.put( tx, lockElements );
            lockElements.add( new LockElement( resource, type ) );
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

    public Collection<Long> getCowRelationshipRemoveMap( NodeImpl node, String type,
        boolean create )
    {
        if ( !create )
        {
            return getCowRelationshipRemoveMap( node, type );
        }
        PrimitiveElement primitiveElement = getAndSetupPrimitiveElement();
        ArrayMap<Long,CowNodeElement> cowElements =
            primitiveElement.nodes;
        CowNodeElement element = cowElements.get( node.getId() );
        if ( element == null )
        {
            element = new CowNodeElement();
            cowElements.put( node.getId(), element );
        }
        if ( element.relationshipRemoveMap == null )
        {
            element.relationshipRemoveMap = new ArrayMap<String,Collection<Long>>();
        }
        Collection<Long> set = element.relationshipRemoveMap.get( type );
        if ( set == null )
        {
            set = new HashSet<Long>();
            element.relationshipRemoveMap.put( type, set );
        }
        return set;
    }

    public ArrayMap<String,RelIdArray> getCowRelationshipAddMap( NodeImpl node )
    {
        PrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement != null )
        {
            ArrayMap<Long,CowNodeElement> cowElements =
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( node.getId() );
            if ( element != null )
            {
                return element.relationshipAddMap;
            }
        }
        return null;
    }

    public RelIdArray getCowRelationshipAddMap( NodeImpl node, String type )
    {
        PrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement != null )
        {
            ArrayMap<Long,CowNodeElement> cowElements =
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( node.getId() );
            if ( element != null && element.relationshipAddMap != null )
            {
                return element.relationshipAddMap.get( type );
            }
        }
        return null;
    }

    public RelIdArray getCowRelationshipAddMap( NodeImpl node, String type,
        boolean create )
    {
        PrimitiveElement primitiveElement = getAndSetupPrimitiveElement();
        ArrayMap<Long,CowNodeElement> cowElements =
            primitiveElement.nodes;
        CowNodeElement element = cowElements.get( node.getId() );
        if ( element == null )
        {
            element = new CowNodeElement();
            cowElements.put( node.getId(), element );
        }
        if ( element.relationshipAddMap == null )
        {
            element.relationshipAddMap = new ArrayMap<String,RelIdArray>();
        }
        RelIdArray set = element.relationshipAddMap.get( type );
        if ( set == null )
        {
            set = new RelIdArrayWithLoops( type );
            element.relationshipAddMap.put( type, set );
        }
        return set;
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
                    if ( lockElement.lockType == LockType.READ )
                    {
                        lockManager.releaseReadLock( lockElement.resource, null );
                    }
                    else if ( lockElement.lockType == LockType.WRITE )
                    {
                        lockManager.releaseWriteLock( lockElement.resource, tx );
                    }
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
                CowNodeElement nodeElement = entry.getValue();
                if ( param == Status.STATUS_COMMITTED )
                {
                    node.commitRelationshipMaps( nodeElement.relationshipAddMap,
                        nodeElement.relationshipRemoveMap );
                    node.commitPropertyMaps( nodeElement.propertyAddMap,
                        nodeElement.propertyRemoveMap );
                }
                else if ( param != Status.STATUS_ROLLEDBACK )
                {
                    throw new TransactionFailureException(
                        "Unknown transaction status: " + param );
                }
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
                CowRelElement relElement = entry.getValue();
                if ( param == Status.STATUS_COMMITTED )
                {
                    rel.commitPropertyMaps( relElement.propertyAddMap,
                        relElement.propertyRemoveMap );
                }
                else if ( param != Status.STATUS_ROLLEDBACK )
                {
                    throw new TransactionFailureException(
                        "Unknown transaction status: " + param );
                }
            }
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
        if ( primitiveElement != null && primitive instanceof NodeImpl )
        {
            ArrayMap<Long,CowNodeElement> cowElements =
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( primitive.getId() );
            if ( element != null )
            {
                if ( element.deleted )
                {
                    throw new IllegalStateException( "Node[" +
                            primitive.getId() + "] has been deleted in this tx" );
                }
                return element.propertyRemoveMap;
            }
        }
        else if ( primitiveElement != null &&
            primitive instanceof RelationshipImpl )
        {
            ArrayMap<Long,CowRelElement> cowElements =
                primitiveElement.relationships;
            CowRelElement element = cowElements.get( primitive.getId() );
            if ( element != null )
            {
                if ( element.deleted )
                {
                    throw new IllegalStateException( "Relationship[" +
                            primitive.getId() + "] has been deleted in this tx" );
                }
                return element.propertyRemoveMap;
            }
        }
        return null;
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyAddMap(
        Primitive primitive )
    {
        PrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement != null && primitive instanceof NodeImpl )
        {
            ArrayMap<Long,CowNodeElement> cowElements =
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( primitive.getId() );
            if ( element != null )
            {
                if ( element.deleted )
                {
                    throw new IllegalStateException( "Node[" +
                            primitive.getId() + "] has been deleted in this tx" );
                }
                return element.propertyAddMap;
            }
        }
        else if ( primitiveElement != null &&
            primitive instanceof RelationshipImpl )
        {
            ArrayMap<Long,CowRelElement> cowElements =
                primitiveElement.relationships;
            CowRelElement element = cowElements.get( primitive.getId() );
            if ( element != null )
            {
                if ( element.deleted )
                {
                    throw new IllegalStateException( "Relationship[" +
                            primitive.getId() + "] has been deleted in this tx" );
                }
                return element.propertyAddMap;
            }
        }
        return null;
    }

    private PrimitiveElement getAndSetupPrimitiveElement()
    {
        Transaction tx = getTransaction();
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        PrimitiveElement primitiveElement = cowMap.get( tx );
        if ( primitiveElement == null )
        {
            primitiveElement = new PrimitiveElement();
            cowMap.put( tx, primitiveElement );
        }
        return primitiveElement;
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyAddMap(
        Primitive primitive, boolean create )
    {
        if ( !create )
        {
            return getCowPropertyAddMap( primitive );
        }
        PrimitiveElement primitiveElement = getAndSetupPrimitiveElement();
        if ( primitive instanceof NodeImpl )
        {
            ArrayMap<Long,CowNodeElement> cowElements =
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( primitive.getId() );
            if ( element != null && element.deleted )
            {
                throw new IllegalStateException( "Node[" +
                        primitive.getId() + "] has been deleted in this tx" );
            }
            if ( element == null )
            {
                element = new CowNodeElement();
                cowElements.put( primitive.getId(), element );
            }
            if ( element.propertyAddMap == null )
            {
                element.propertyAddMap = new ArrayMap<Integer,PropertyData>();
            }
            return element.propertyAddMap;
        }
        else if ( primitive instanceof RelationshipImpl )
        {
            ArrayMap<Long,CowRelElement> cowElements =
                primitiveElement.relationships;
            CowRelElement element = cowElements.get( primitive.getId() );
            if ( element != null && element.deleted )
            {
                throw new IllegalStateException( "Relationship[" +
                        primitive.getId() + "] has been deleted in this tx" );
            }
            if ( element == null )
            {
                element = new CowRelElement();
                cowElements.put( primitive.getId(), element );
            }
            if ( element.propertyAddMap == null )
            {
                element.propertyAddMap = new ArrayMap<Integer,PropertyData>();
            }
            return element.propertyAddMap;
        }
        return null;
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyRemoveMap(
        Primitive primitive, boolean create )
    {
        if ( !create )
        {
            return getCowPropertyRemoveMap( primitive );
        }
        PrimitiveElement primitiveElement = getAndSetupPrimitiveElement();
        if ( primitive instanceof NodeImpl )
        {
            ArrayMap<Long,CowNodeElement> cowElements =
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( primitive.getId() );
            if ( element != null && element.deleted )
            {
                throw new IllegalStateException( "Node[" +
                        primitive.getId() + "] has been deleted in this tx" );
            }
            if ( element == null )
            {
                element = new CowNodeElement();
                cowElements.put( primitive.getId(), element );
            }
            if ( element.propertyRemoveMap == null )
            {
                element.propertyRemoveMap = new ArrayMap<Integer,PropertyData>();
            }
            return element.propertyRemoveMap;
        }
        else if ( primitive instanceof RelationshipImpl )
        {
            ArrayMap<Long,CowRelElement> cowElements =
                primitiveElement.relationships;
            CowRelElement element = cowElements.get( primitive.getId() );
            if ( element != null && element.deleted )
            {
                throw new IllegalStateException( "Relationship[" +
                        primitive.getId() + "] has been deleted in this tx" );
            }
            if ( element == null )
            {
                element = new CowRelElement();
                cowElements.put( primitive.getId(), element );
            }
            if ( element.propertyRemoveMap == null )
            {
                element.propertyRemoveMap = new ArrayMap<Integer,PropertyData>();
            }
            return element.propertyRemoveMap;
        }
        return null;
    }

    public void deletePrimitive( Primitive primitive )
    {
        PrimitiveElement primitiveElement = getAndSetupPrimitiveElement();
        if ( primitive instanceof NodeImpl )
        {
            ArrayMap<Long,CowNodeElement> cowElements =
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( primitive.getId() );
            if ( element != null && element.deleted )
            {
                throw new IllegalStateException( "Node[" +
                        primitive.getId() + "] has already been deleted in this tx" );
            }
            if ( element == null )
            {
                element = new CowNodeElement();
                cowElements.put( primitive.getId(), element );
            }
            element.deleted = true;
        }
        else if ( primitive instanceof RelationshipImpl )
        {
            ArrayMap<Long,CowRelElement> cowElements =
                primitiveElement.relationships;
            CowRelElement element = cowElements.get( primitive.getId() );
            if ( element != null && element.deleted )
            {
                throw new IllegalStateException( "Relationship[" +
                        primitive.getId() + "] has already been deleted in this tx" );
            }
            if ( element == null )
            {
                element = new CowRelElement();
                cowElements.put( primitive.getId(), element );
            }
            element.deleted = true;
        }
    }

    public void removeNodeFromCache( long nodeId )
    {
        if ( nodeManager != null )
        {
            nodeManager.removeNodeFromCache( nodeId );
        }
    }

    public void addRelationshipType( RelationshipTypeData type )
    {
        if ( nodeManager != null )
        {
            nodeManager.addRelationshipType( type );
        }
    }

    public void addPropertyIndex( PropertyIndexData index )
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
            RelationshipProxy rel = new RelationshipProxy( relId, nodeManager );
            RelationshipImpl relImpl = nodeManager.getRelForProxy( rel, null );
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
            NodeProxy node = new NodeProxy( nodeId, nodeManager );
            NodeImpl nodeImpl = nodeManager.getNodeForProxy( node, null );
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
                        RelationshipProxy rel = new RelationshipProxy( relId, nodeManager );
                        if ( rel.getStartNode().getId() == nodeId )
                        {
                            result.deleted( new RelationshipProxy( relId, nodeManager ) );
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
            RelationshipProxy rel = new RelationshipProxy( relId, nodeManager );
            if ( rel.getStartNode().getId() == nodeId )
            {
                result.created( new RelationshipProxy( relId, nodeManager ) );
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
            result.created( new NodeProxy( nodeId, nodeManager ) );
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