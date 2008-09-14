/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
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
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.api.core.NotInTransactionException;
import org.neo4j.impl.transaction.LockManager;
import org.neo4j.impl.transaction.LockType;
import org.neo4j.impl.util.ArrayIntSet;
import org.neo4j.impl.util.ArrayMap;

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

    private final NodeManager nodeManager;
    private final LockManager lockManager;
    private final TransactionManager transactionManager;

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

        ArrayMap<String,ArrayIntSet> relationshipAddMap = null;
        ArrayMap<String,ArrayIntSet> relationshipRemoveMap = null;
        ArrayMap<Integer,Property> propertyAddMap = null;
        ArrayMap<Integer,Property> propertyRemoveMap = null;
    }

    private static class CowRelElement
    {
        CowRelElement()
        {

        }

        ArrayMap<Integer,Property> propertyAddMap = null;
        ArrayMap<Integer,Property> propertyRemoveMap = null;
    }

    public LockReleaser( LockManager lockManager,
        TransactionManager transactionManager, NodeManager nodeManager )
    {
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.nodeManager = nodeManager;
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
        try
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
                tx.registerSynchronization( new TxCommitHook( this, tx ) );
                lockElements = new ArrayList<LockElement>();
                lockMap.put( tx, lockElements );
                lockElements.add( new LockElement( resource, type ) );
            }
        }
        catch ( javax.transaction.SystemException e )
        {
            throw new NotInTransactionException( e );
        }
        catch ( Exception e )
        {
            throw new NotInTransactionException( e );
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
            throw new RuntimeException();
        }
    }

    public ArrayIntSet getCowRelationshipRemoveMap( NodeImpl node, String type )
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

    public ArrayIntSet getCowRelationshipRemoveMap( NodeImpl node, String type,
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
            element.relationshipRemoveMap = new ArrayMap<String,ArrayIntSet>();
        }
        ArrayIntSet set = element.relationshipRemoveMap.get( type );
        if ( set == null )
        {
            set = new ArrayIntSet();
            element.relationshipRemoveMap.put( type, set );
        }
        return set;
    }

    public ArrayMap<String,ArrayIntSet> getCowRelationshipAddMap( NodeImpl node )
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

    public ArrayIntSet getCowRelationshipAddMap( NodeImpl node, String type )
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

    public ArrayIntSet getCowRelationshipAddMap( NodeImpl node, String type,
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
            element.relationshipAddMap = new ArrayMap<String,ArrayIntSet>();
        }
        ArrayIntSet set = element.relationshipAddMap.get( type );
        if ( set == null )
        {
            set = new ArrayIntSet();
            element.relationshipAddMap.put( type, set );
        }
        return set;
    }

    public void releaseLocks( Transaction tx )
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

    public void releaseCows( Transaction cowTxId, int param )
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
                    throw new RuntimeException( "Unkown status: " + param );
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
                    throw new RuntimeException( "Unkown status: " + param );
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

    public ArrayMap<Integer,Property> getCowPropertyRemoveMap(
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

    public ArrayMap<Integer,Property> getCowPropertyAddMap(
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
    
    public ArrayMap<Integer,Property> getCowPropertyAddMap(
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
                element.propertyAddMap = new ArrayMap<Integer,Property>();
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
                element.propertyAddMap = new ArrayMap<Integer,Property>();
            }
            return element.propertyAddMap;
        }
        return null;
    }

    public ArrayMap<Integer,Property> getCowPropertyRemoveMap(
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
                element.propertyRemoveMap = new ArrayMap<Integer,Property>();
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
                element.propertyRemoveMap = new ArrayMap<Integer,Property>();
            }
            return element.propertyRemoveMap;
        }
        return null;
    }
}