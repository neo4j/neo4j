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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
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
 */
public interface TransactionState
{
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

    ArrayMap<Integer, PropertyData> getCowPropertyRemoveMap( Primitive primitive );

    ArrayMap<Integer, PropertyData> getCowPropertyAddMap( Primitive primitive );

    PrimitiveElement getPrimitiveElement();

    PrimitiveElement getOrCreatePrimitiveElement();

    ArrayMap<Integer, PropertyData> getOrCreateCowPropertyAddMap(
            Primitive primitive );

    ArrayMap<Integer, PropertyData> getOrCreateCowPropertyRemoveMap(
            Primitive primitive );

    void deletePrimitive( Primitive primitive );

    void removeNodeFromCache( long nodeId );

    void addRelationshipType( NameData type );

    void addPropertyIndex( NameData index );

    void removeRelationshipFromCache( long id );

    /**
     * Patches the relationship chain loading parts of the start and end nodes of deleted relationships. This is
     * a good idea to call when deleting relationships, otherwise the in memory representation of relationship chains
     * may become damaged.
     * This is not expected to remove the deleted relationship from the cache - use
     * {@link #removeRelationshipFromCache(long)} for that purpose before calling this method.
     *
     * @param relId The relId of the relationship deleted
     * @param firstNodeId The relId of the first node
     * @param firstNodeNextRelId The next relationship relId of the first node in its relationship chain
     * @param secondNodeId The relId of the second node
     * @param secondNodeNextRelId The next relationship relId of the second node in its relationship chain
     */
    void patchDeletedRelationshipNodes( long relId, long firstNodeId, long firstNodeNextRelId, long secondNodeId,
                                      long secondNodeNextRelId );

    void removeRelationshipTypeFromCache( int id );

    void removeGraphPropertiesFromCache();

    void clearCache();

    TransactionData getTransactionData();
    
    void addPropertyIndex( PropertyIndex index );

    PropertyIndex getPropertyIndex( String key );

    PropertyIndex getPropertyIndex( int keyId );
    
    boolean isDeleted( Node node );

    boolean isDeleted( Relationship relationship );
    
    PropertyIndex[] getAddedPropertyIndexes();
    
    boolean hasChanges();
    
    void setRollbackOnly();
    
    public RemoteTxHook getTxHook();
    
    public TxIdGenerator getTxIdGenerator();
    
    public static final TransactionState NO_STATE = new NoTransactionState();

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
