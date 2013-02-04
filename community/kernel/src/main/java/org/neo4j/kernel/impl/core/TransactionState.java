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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.NameData;
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

    PrimitiveElement getPrimitiveElement( boolean create );

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
    
    public TxHook getTxHook();
    
    public TxIdGenerator getTxIdGenerator();
    
    public static final TransactionState NO_STATE = new NoTransactionState();
}
