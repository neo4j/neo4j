/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Set;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.persistence.PersistenceManager.ResourceHolder;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;

/**
 * Keeps transaction state for a single transaction, such as:
 * <ul>
 *   <li>Created nodes and relationships</li>
 *   <li>Added, modified and deleted properties</li>
 *   <li>Created relationship types and property indexes</li>
 *   <li>Held locks</li>
 * </ul>
 * @author Mattias
 *
 * This is slowly being replaced / merged with the new KernelTransaction transaction state,
 * {@link org.neo4j.kernel.impl.api.state.TxState}, please avoid adding more functionality to this class.
 */
public interface TransactionState
{
    TransactionState NO_STATE = new NoTransactionState();

    LockElement acquireWriteLock( Object resource );

    LockElement acquireReadLock( Object resource );

    ArrayMap<Integer, RelIdArray> getCowRelationshipAddMap( NodeImpl node );

    RelIdArray getOrCreateCowRelationshipAddMap( NodeImpl node, int type );

    ArrayMap<Integer, Collection<Long>> getCowRelationshipRemoveMap( NodeImpl node );

    Collection<Long> getOrCreateCowRelationshipRemoveMap( NodeImpl node, int type );

    void setFirstIds( long nodeId, long firstRel, long firstProp );

    void commit();

    void commitCows();

    void rollback();

    boolean hasLocks();

    ArrayMap<Integer, DefinedProperty> getCowPropertyRemoveMap( Primitive primitive );

    ArrayMap<Integer, DefinedProperty> getCowPropertyAddMap( Primitive primitive );

    ArrayMap<Integer, DefinedProperty> getOrCreateCowPropertyAddMap( Primitive primitive );

    ArrayMap<Integer, DefinedProperty> getOrCreateCowPropertyRemoveMap( Primitive primitive );

    void createNode( long id );

    void createRelationship( long id );

    void deleteNode( long id );

    void deleteRelationship( long id );

    TransactionData getTransactionData();

    boolean nodeIsDeleted( long nodeId );

    boolean relationshipIsDeleted( long relationshpId );

    boolean hasChanges();

    RemoteTxHook getTxHook();

    TxIdGenerator getTxIdGenerator();

    Set<Long> getCreatedNodes();

    Set<Long> getCreatedRelationships();

    // Tech debt, this is here waiting for transaction state to move to the TxState class
    Iterable<WritableTransactionState.CowNodeElement> getChangedNodes();
    
    /**
     * Below are two methods for getting and setting a {@link ResourceHolder}, i.e. a carrier of a
     * {@link NeoStoreTransaction}. This is not a very good strategy. The reason it's here is that it's
     * less contended to put and reach that instance in each {@link TransactionState} object, instead of
     * in a shared map or similar in {@link PersistenceManager}.
     */
    ResourceHolder getNeoStoreTransaction();
    
    void setNeoStoreTransaction( ResourceHolder neoStoreTransaction );

    /**
     * A history of slave transactions and their cultural impact on Graph Databases.
     *
     *
     * Acknowledgments
     *
     * I would like to thank both Mattias and Chris for their excellent input on this subject, without their eye
     * witness accounts of the actual events, none of this would be possible.
     *
     *
     * Chapter I
     * Humble Beginnings
     *
     * Once upon a time, when a slave asked the master to perform an action, this used to imply something, a bond.
     * The master recognized the slaves need for a transaction, and would implicitly create one. It was a time of peace
     * and of reconciliation. Alas, this soon led to confusion as untrustworthy networks would cause master switches.
     * A new master might receive a message intended for another, and would out of the kindness of his kernel create a
     * new transaction for the slave. While a beautiful gesture, it shouldn't have done this because now it had tore
     * transaction state into two transactions, a most vile abomination.
     *
     * So it was decided that the trust the masters had in their slaves must be revoked. Instead slaves had to
     * explicitly ask the masters to create a transaction. All was now consistent - but now an additional network hop
     * was required, and slaves begin transactions on the masters for the most simple of requests. Read only operations
     * would suddenly pull updates. The universe was in disarray.
     *
     *
     * Chapter II
     * A hack
     *
     * Pulling updates on read operations is very bad, because it voids the databases contracts for when updates should
     * be pulled, and makes read scaling perform very poorly. It was soon recognized that, while correct, the new
     * regimen could not be allowed to stand. A hack was devised, whereupon the slave would wait to initialize the
     * transaction on the master until the first write lock was required. A clear signal that a write was about to
     * happen.
     *
     * This very hack is why the below methods exist.
     *
     *
     * Chapter III
     * A new dawn
     *
     * Once all Neo4j operations are contained in the Kernel component, the kernel will be able to get a clear view of
     * all running transactions, and clear entry points for all running operations within those transactions. With that,
     * a new rein of trust can be implemented. As soon as a slave sees a new master, it will be able to void all running
     * transactions, and start new with the new master. This will allow a return to the days of implicit transactions,
     * where network communication is done only just when it is needed, and removing the additional network hop.
     *
     * The End.
     *
     */
    boolean isRemotelyInitialized();

    void markAsRemotelyInitialized();
}
