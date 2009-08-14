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
package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.Logger;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.api.core.NotInTransactionException;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.util.ArrayMap;
import org.neo4j.impl.util.IntArray;

/**
 * Manages object version diffs and locks for each transaction.
 */
public class LockReleaser
{
    private static Logger log = Logger.getLogger( LockReleaser.class.getName() );

    private final ArrayMap<Transaction,List<LockElement>> lockMap = 
        new ArrayMap<Transaction,List<LockElement>>( 5, true, true );
    private final ArrayMap<Transaction,NeoPrimitiveElement> cowMap = 
        new ArrayMap<Transaction,NeoPrimitiveElement>( 5, true, true );

    private NodeManager nodeManager;
    private final LockManager lockManager;
    private final TransactionManager transactionManager;
    private PropertyIndexManager propertyIndexManager; 
    
    private static class NeoPrimitiveElement
    {
        NeoPrimitiveElement()
        {
        }

        final ArrayMap<Integer,CowNodeElement> nodes = 
            new ArrayMap<Integer,CowNodeElement>();
        final ArrayMap<Integer,CowRelElement> relationships = 
            new ArrayMap<Integer,CowRelElement>();
    }

    private static class CowNodeElement
    {
        CowNodeElement()
        {

        }

        ArrayMap<String,IntArray> relationshipAddMap = null;
        ArrayMap<String,IntArray> relationshipRemoveMap = null;
        ArrayMap<Integer,PropertyData> propertyAddMap = null;
        ArrayMap<Integer,PropertyData> propertyRemoveMap = null;
    }

    private static class CowRelElement
    {
        CowRelElement()
        {

        }

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
                    lockManager.releaseWriteLock( resource );
                }
                else if ( type == LockType.READ )
                {
                    lockManager.releaseReadLock( resource );
                }
                return;
            }
            lockElements = new ArrayList<LockElement>();
            lockMap.put( tx, lockElements );
            lockElements.add( new LockElement( resource, type ) );
            // we have to have a syncrhonization hook for read only transaction,
            // write locks can be taken in read only transactions (ex: 
            // transactions that peform write operations that cancel each other
            // out). This sync hook will only release locks if they exist and 
            // tx was read only
            try
            {
                tx.registerSynchronization( new ReadOnlyTxReleaser( tx ) );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
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
            throw new RuntimeException( "Unable to get transaction", e );
        }
    }

    public IntArray getCowRelationshipRemoveMap( NodeImpl node, String type )
    {
        NeoPrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement != null )
        {
            ArrayMap<Integer,CowNodeElement> cowElements = 
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( node.id );
            if ( element != null && element.relationshipRemoveMap != null )
            {
                return element.relationshipRemoveMap.get( type );
            }
        }
        return null;
    }

    public IntArray getCowRelationshipRemoveMap( NodeImpl node, String type,
        boolean create )
    {
        if ( !create )
        {
            return getCowRelationshipRemoveMap( node, type );
        }
        NeoPrimitiveElement primitiveElement = getAndSetupPrimitiveElement();
        ArrayMap<Integer,CowNodeElement> cowElements = 
            primitiveElement.nodes;
        CowNodeElement element = cowElements.get( node.id );
        if ( element == null )
        {
            element = new CowNodeElement();
            cowElements.put( node.id, element );
        }
        if ( element.relationshipRemoveMap == null )
        {
            element.relationshipRemoveMap = new ArrayMap<String,IntArray>();
        }
        IntArray set = element.relationshipRemoveMap.get( type );
        if ( set == null )
        {
            set = new IntArray();
            element.relationshipRemoveMap.put( type, set );
        }
        return set;
    }

    public ArrayMap<String,IntArray> getCowRelationshipAddMap( NodeImpl node )
    {
        NeoPrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement != null )
        {
            ArrayMap<Integer,CowNodeElement> cowElements = 
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( node.id );
            if ( element != null )
            {
                return element.relationshipAddMap;
            }
        }
        return null;
    }

    public IntArray getCowRelationshipAddMap( NodeImpl node, String type )
    {
        NeoPrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement != null )
        {
            ArrayMap<Integer,CowNodeElement> cowElements = 
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( node.id );
            if ( element != null && element.relationshipAddMap != null )
            {
                return element.relationshipAddMap.get( type );
            }
        }
        return null;
    }

    public IntArray getCowRelationshipAddMap( NodeImpl node, String type,
        boolean create )
    {
        if ( !create )
        {
            return getCowRelationshipRemoveMap( node, type );
        }
        NeoPrimitiveElement primitiveElement = getAndSetupPrimitiveElement();
        ArrayMap<Integer,CowNodeElement> cowElements = 
            primitiveElement.nodes;
        CowNodeElement element = cowElements.get( node.id );
        if ( element == null )
        {
            element = new CowNodeElement();
            cowElements.put( node.id, element );
        }
        if ( element.relationshipAddMap == null )
        {
            element.relationshipAddMap = new ArrayMap<String,IntArray>();
        }
        IntArray set = element.relationshipAddMap.get( type );
        if ( set == null )
        {
            set = new IntArray();
            element.relationshipAddMap.put( type, set );
        }
        return set;
    }

    public void commit()
    {
        Transaction tx = getTransaction();
        // propertyIndex
        propertyIndexManager.commit( tx );
        releaseCows( tx, Status.STATUS_COMMITTED );
        releaseLocks( tx );
    }
    
    public void rollback()
    {
        Transaction tx = getTransaction();
        // propertyIndex
        propertyIndexManager.rollback( tx );
        releaseCows( tx, Status.STATUS_ROLLEDBACK );
        releaseLocks( tx );
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
                        lockManager.releaseReadLock( lockElement.resource );
                    }
                    else if ( lockElement.lockType == LockType.WRITE )
                    {
                        lockManager.releaseWriteLock( lockElement.resource );
                    }
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    log.severe( "Unable to release lock[" + 
                        lockElement.lockType + "] on resource[" + 
                        lockElement.resource + "]" );
                }
            }
        }
    }

    void releaseCows( Transaction cowTxId, int param )
    {
        NeoPrimitiveElement element = cowMap.remove( cowTxId );
        if ( element == null )
        {
            return;
        }
        ArrayMap<Integer,CowNodeElement> cowNodeElements = element.nodes;
        Set<Entry<Integer,CowNodeElement>> nodeEntrySet = 
            cowNodeElements.entrySet();
        for ( Entry<Integer,CowNodeElement> entry : nodeEntrySet )
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
                    throw new RuntimeException( "Unknown status: " + param );
                }
            }
        }
        ArrayMap<Integer,CowRelElement> cowRelElements = element.relationships;
        Set<Entry<Integer,CowRelElement>> relEntrySet = 
            cowRelElements.entrySet();
        for ( Entry<Integer,CowRelElement> entry : relEntrySet )
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
                    throw new RuntimeException( "Unknown status: " + param );
                }
            }
        }
        cowMap.remove( cowTxId );
    }

    public synchronized void dumpLocks()
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
        NeoPrimitive primitive )
    {
        NeoPrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement != null && primitive instanceof NodeImpl )
        {
            ArrayMap<Integer,CowNodeElement> cowElements = 
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( primitive.id );
            if ( element != null )
            {
                return element.propertyRemoveMap;
            }
        }
        else if ( primitiveElement != null && 
            primitive instanceof RelationshipImpl )
        {
            ArrayMap<Integer,CowRelElement> cowElements = 
                primitiveElement.relationships;
            CowRelElement element = cowElements.get( primitive.id );
            if ( element != null )
            {
                return element.propertyRemoveMap;
            }
        }
        return null;
    }

    public ArrayMap<Integer,PropertyData> getCowPropertyAddMap(
        NeoPrimitive primitive )
    {
        NeoPrimitiveElement primitiveElement = cowMap.get( getTransaction() );
        if ( primitiveElement != null && primitive instanceof NodeImpl )
        {
            ArrayMap<Integer,CowNodeElement> cowElements = 
                primitiveElement.nodes; 
            CowNodeElement element = cowElements.get( primitive.id );
            if ( element != null )
            {
                return element.propertyAddMap;
            }
        }
        else if ( primitiveElement != null && 
            primitive instanceof RelationshipImpl )
        {
            ArrayMap<Integer,CowRelElement> cowElements = 
                primitiveElement.relationships; 
            CowRelElement element = cowElements.get( primitive.id );
            if ( element != null )
            {
                return element.propertyAddMap;
            }
        }
        return null;
    }

    private NeoPrimitiveElement getAndSetupPrimitiveElement()
    {
        Transaction tx = getTransaction();
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        NeoPrimitiveElement primitiveElement = cowMap.get( tx );
        if ( primitiveElement == null )
        {
            primitiveElement = new NeoPrimitiveElement();
            cowMap.put( tx, primitiveElement );
        }
        return primitiveElement;
    }
    
    public ArrayMap<Integer,PropertyData> getCowPropertyAddMap(
        NeoPrimitive primitive, boolean create )
    {
        if ( !create )
        {
            return getCowPropertyAddMap( primitive );
        }
        NeoPrimitiveElement primitiveElement = getAndSetupPrimitiveElement();
        if ( primitive instanceof NodeImpl )
        {
            ArrayMap<Integer,CowNodeElement> cowElements = 
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( primitive.id );
            if ( element == null )
            {
                element = new CowNodeElement();
                cowElements.put( primitive.id, element );
            }
            if ( element.propertyAddMap == null )
            {
                element.propertyAddMap = new ArrayMap<Integer,PropertyData>();
            }
            return element.propertyAddMap;
        }
        else if ( primitive instanceof RelationshipImpl )
        {
            ArrayMap<Integer,CowRelElement> cowElements = 
                primitiveElement.relationships;
            CowRelElement element = cowElements.get( primitive.id );
            if ( element == null )
            {
                element = new CowRelElement();
                cowElements.put( primitive.id, element );
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
        NeoPrimitive primitive, boolean create )
    {
        if ( !create )
        {
            return getCowPropertyRemoveMap( primitive );
        }
        NeoPrimitiveElement primitiveElement = getAndSetupPrimitiveElement();
        if ( primitive instanceof NodeImpl )
        {
            ArrayMap<Integer,CowNodeElement> cowElements = 
                primitiveElement.nodes;
            CowNodeElement element = cowElements.get( primitive.id );
            if ( element == null )
            {
                element = new CowNodeElement();
                cowElements.put( primitive.id, element );
            }
            if ( element.propertyRemoveMap == null )
            {
                element.propertyRemoveMap = new ArrayMap<Integer,PropertyData>();
            }
            return element.propertyRemoveMap;
        }
        else if ( primitive instanceof RelationshipImpl )
        {
            ArrayMap<Integer,CowRelElement> cowElements = 
                primitiveElement.relationships;
            CowRelElement element = cowElements.get( primitive.id );
            if ( element == null )
            {
                element = new CowRelElement();
                cowElements.put( primitive.id, element );
            }
            if ( element.propertyRemoveMap == null )
            {
                element.propertyRemoveMap = new ArrayMap<Integer,PropertyData>();
            }
            return element.propertyRemoveMap;
        }
        return null;
    }

    public void removeNodeFromCache( int nodeId )
    {
        nodeManager.removeNodeFromCache( nodeId );
    }

    public void removeRelationshipFromCache( int id )
    {
        nodeManager.removeRelationshipFromCache( id );
    }
    
    public void removeRelationshipTypeFromCache( int id )
    {
        nodeManager.removeRelationshipTypeFromCache( id );
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
            // TODO Auto-generated method stub
            
        }
    }
}