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
package org.neo4j.kernel.impl.core;

import java.util.Collection;
import java.util.Set;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.transaction.TxHook;
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

    void dumpLocks();

    ArrayMap<Integer, PropertyData> getCowPropertyRemoveMap( Primitive primitive );

    ArrayMap<Integer, PropertyData> getCowPropertyAddMap( Primitive primitive );

    ArrayMap<Integer, PropertyData> getOrCreateCowPropertyAddMap(
            Primitive primitive );

    ArrayMap<Integer, PropertyData> getOrCreateCowPropertyRemoveMap(
            Primitive primitive );
    
    void createNode( long id );

    void createRelationship( long id );

    void deleteNode( long id );

    void deleteRelationship( long id );

    TransactionData getTransactionData();
    
    boolean nodeIsDeleted( long nodeId );
    
    boolean relationshipIsDeleted( long relationshpId );
    
    boolean hasChanges();
    
    void setRollbackOnly();
    
    public TxHook getTxHook();
    
    public TxIdGenerator getTxIdGenerator();
    
    Set<Long> getCreatedNodes();
    
    Set<Long> getCreatedRelationships();

    Set<Long> getDeletedNodes();

    // Tech debt, this is here waiting for transaction state to move to the TxState class
    Iterable<WritableTransactionState.CowNodeElement> getChangedNodes();
    
    public static final TransactionState NO_STATE = new NoTransactionState();
}
