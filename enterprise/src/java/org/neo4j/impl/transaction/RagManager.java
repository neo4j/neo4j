/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
package org.neo4j.impl.transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.impl.util.ArrayMap;

/**
 * The Resource Allocation Graph manager is used for deadlock detection. It
 * keeps track of all locked resources and transactions waiting for resources.
 * When a {@link RWLock} cannot give the lock to a transaction the tx has to
 * wait and that may lead to a deadlock. So before the tx is put into wait mode
 * the {@link RagManager#checkWaitOn} method is invoked to check if a wait of
 * this transaction will lead to a deadlock.
 * <p>
 * The <CODE>checkWaitOn</CODE> throws a {@link DeadlockDetectedException} if
 * a deadlock would occur when the transaction would wait for the resource. That
 * will guarantee that a deadlock never occurs on a RWLock basis.
 * <p>
 * Think of the resource allocation graph as a node space. We have two node
 * types, resource nodes (R) and tx/process nodes (T). When a transaction
 * acquires lock on some resource a relationship is added from the resource to
 * the tx (R->T) and when a transaction waits for a resource a relationship is
 * added from the tx to the resource (T->R). The only thing we need to do to see
 * if a deadlock occurs when some transaction waits for a resource is to
 * traverse node nodespace starting on the resource and see if we can get back
 * to the tx ( T1 wants to wait on R1 and R1->T2->R2->T3->R8->T1 <==>
 * deadlock!).
 */
class RagManager
{
    // if a runtime exception is thrown from any method it means that the
    // RWLock class hasn't kept the contract to the RagManager
    // The contract is:
    // o When a transaction gets a lock on a resource and both the readCount and
    // writeCount for that transaction on the resource was 0
    // RagManager.lockAcquired( resource ) must be invoked
    // o When a tx releases a lock on a resource and both the readCount and
    // writeCount for that transaction on the resource goes down to zero
    // RagManager.lockReleased( resource ) must be invoked
    // o After invoke to the checkWaitOn( resource ) method that didn't result
    // in a DeadlockDetectedException the transaction must wait
    // o When the transaction wakes up from waiting on a resource the
    // stopWaitOn( resource ) method must be invoked

    private final Map<Object,List<Transaction>> resourceMap = 
        new HashMap<Object,List<Transaction>>();

    private final ArrayMap<Transaction,Object> waitingTxMap = 
        new ArrayMap<Transaction,Object>( 5, false, true );

    private final TransactionManager tm;

    RagManager( TransactionManager tm )
    {
        this.tm = tm;
    }

    synchronized void lockAcquired( Object resource, Transaction tx )
    {
        List<Transaction> lockingTxList = resourceMap.get( resource );
        if ( lockingTxList != null )
        {
            assert !lockingTxList.contains( tx );
            lockingTxList.add( tx );
        }
        else
        {
            lockingTxList = new LinkedList<Transaction>();
            lockingTxList.add( tx );
            resourceMap.put( resource, lockingTxList );
        }
    }

    synchronized void lockReleased( Object resource, Transaction tx )
    {
        List<Transaction> lockingTxList = resourceMap.get( resource );
        if ( lockingTxList == null )
        {
            throw new RuntimeException( resource + " not found in resource map" );
        }

        if ( !lockingTxList.remove( tx ) )
        {
            throw new RuntimeException( tx + "not found in locking tx list" );
        }
        if ( lockingTxList.size() == 0 )
        {
            resourceMap.remove( resource );
        }
    }

    synchronized void stopWaitOn( Object resource, Transaction tx )
    {
        if ( waitingTxMap.remove( tx ) == null )
        {
            throw new RuntimeException( tx + " not waiting on " + resource );
        }
    }

    // after invoke the transaction must wait on the resource
    synchronized void checkWaitOn( Object resource, Transaction tx )
        throws DeadlockDetectedException
    {
        List<Transaction> lockingTxList = resourceMap.get( resource );
        if ( lockingTxList == null )
        {
            throw new RuntimeException( "Illegal resource[" + resource
                + "], not found in map" );
        }

        if ( waitingTxMap.get( tx ) != null )
        {
            throw new RuntimeException( tx + " already waiting for resource" );
        }

        Iterator<Transaction> itr = lockingTxList.iterator();
        List<Transaction> checkedTransactions = new LinkedList<Transaction>();
        Stack<Object> graphStack = new Stack<Object>();
        // has resource,transaction interleaved
        graphStack.push( resource );
        while ( itr.hasNext() )
        {
            Transaction lockingTx = itr.next();
            // the if statement bellow is valid because:
            // t1 -> r1 -> t1 (can happened with RW locks) is ok but,
            // t1 -> r1 -> t1&t2 where t2 -> r1 is a deadlock
            // think like this, we have two transactions and one resource
            // o t1 takes read lock on r1
            // o t2 takes read lock on r1
            // o t1 wanna take write lock on r1 but has to wait for t2
            // to release the read lock ( t1->r1->(t1&t2), ok not deadlock yet
            // o t2 wanna take write lock on r1 but has to wait for t1
            // to release read lock....
            // DEADLOCK t1->r1->(t1&t2) and t2->r1->(t1&t2) ===>
            // t1->r1->t2->r1->t1, t2->r1->t1->r1->t2 etc...
            // to allow the first three steps above we check if lockingTx ==
            // waitingTx on first level.
            // because of this special case we have to keep track on the
            // already "checked" tx since it is (now) legal for one type of
            // circular reference to exist (t1->r1->t1) otherwise we may
            // traverse t1->r1->t2->r1->t2->r1->t2... until SOE
            // ... KISS to you too
            if ( lockingTx.equals( tx ) )
            {
                continue;
            }
            graphStack.push( tx );
            checkWaitOnRecursive( lockingTx, tx, checkedTransactions,
                graphStack );
            graphStack.pop();
        }

        // ok no deadlock, we can wait on resource
        waitingTxMap.put( tx, resource );
    }

    private synchronized void checkWaitOnRecursive( Transaction lockingTx,
        Transaction waitingTx, List<Transaction> checkedTransactions,
        Stack<Object> graphStack ) throws DeadlockDetectedException
    {
        if ( lockingTx.equals( waitingTx ) )
        {
            StringBuffer circle = null;
            Object resource = null;
            do
            {
                lockingTx = (Transaction) graphStack.pop();
                resource = graphStack.pop();
                if ( circle == null )
                {
                    circle = new StringBuffer();
                    circle.append( lockingTx + " <- " + resource );
                }
                else
                {
                    circle.append( " <- " + lockingTx + " <- " + resource );
                }
            }
            while ( !graphStack.isEmpty() );
            throw new DeadlockDetectedException( waitingTx + 
                " can't wait on resource " + resource + " since => " + circle );
        }
        checkedTransactions.add( lockingTx );
        Object resource = waitingTxMap.get( lockingTx );
        if ( resource != null )
        {
            graphStack.push( resource );
            // if the resource doesn't exist in resorceMap that means all the
            // locks on the resource has been released
            // it is possible when this tx was in RWLock.acquire and
            // saw it had to wait for the lock the scheduler changes to some
            // other tx that will release the locks on the resource and
            // remove it from the map
            // this is ok since current tx or any other tx will wake
            // in the synchronized block and will be forced to do the deadlock
            // check once more if lock cannot be acquired
            List<Transaction> lockingTxList = resourceMap.get( resource );
            if ( lockingTxList != null )
            {
                Iterator<Transaction> itr = lockingTxList.iterator();
                while ( itr.hasNext() )
                {
                    lockingTx = itr.next();
                    // so we don't
                    if ( !checkedTransactions.contains( lockingTx ) )
                    {
                        graphStack.push( lockingTx );
                        checkWaitOnRecursive( lockingTx, waitingTx,
                            checkedTransactions, graphStack );
                        graphStack.pop();
                    }
                }
            }
            graphStack.pop();
        }
    }

    synchronized void dumpStack()
    {
        System.out.print( "Waiting list: " );
        Iterator<Transaction> transactions = waitingTxMap.keySet().iterator();
        if ( !transactions.hasNext() )
        {
            System.out.println( "No transactions waiting on resources" );
        }
        else
        {
            System.out.println();
        }
        while ( transactions.hasNext() )
        {
            Transaction tx = transactions.next();
            System.out.println( "" + tx + "->" + waitingTxMap.get( tx ) );
        }
        System.out.print( "Resource lock list: " );
        Iterator<?> resources = resourceMap.keySet().iterator();
        if ( !resources.hasNext() )
        {
            System.out.println( "No locked resources found" );
        }
        else
        {
            System.out.println();
        }
        while ( resources.hasNext() )
        {
            Object resource = resources.next();
            System.out.print( "" + resource + "->" );
            Iterator<Transaction> itr = resourceMap.get( resource ).iterator();
            if ( !itr.hasNext() )
            {
                System.out.println( " Error empty list found" );
            }
            while ( itr.hasNext() )
            {
                System.out.print( "" + itr.next() );
                if ( itr.hasNext() )
                {
                    System.out.print( "," );
                }
                else
                {
                    System.out.println();
                }
            }
        }
    }

    Transaction getCurrentTransaction()
    {
        try
        {
            return tm.getTransaction();
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }
}