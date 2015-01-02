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

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.persistence.PersistenceManager.ResourceHolder;
import org.neo4j.kernel.impl.transaction.RemoteTxHook;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;

import static java.util.Collections.emptySet;

public class NoTransactionState implements TransactionState
{
    @Override
    public LockElement acquireWriteLock( Object resource )
    {
        throw new NotInTransactionException();
    }

    @Override
    public LockElement acquireReadLock( Object resource )
    {
        throw new NotInTransactionException();
    }

    @Override
    public ArrayMap<Integer, Collection<Long>> getCowRelationshipRemoveMap( NodeImpl node )
    {
        return null;
    }

    @Override
    public Collection<Long> getOrCreateCowRelationshipRemoveMap( NodeImpl node, int type )
    {
        throw new NotInTransactionException();
    }

    @Override
    public void setFirstIds( long nodeId, long firstRel, long firstProp )
    {
    }

    @Override
    public ArrayMap<Integer, RelIdArray> getCowRelationshipAddMap( NodeImpl node )
    {
        return null;
    }

    @Override
    public RelIdArray getOrCreateCowRelationshipAddMap( NodeImpl node, int type )
    {
        throw new NotInTransactionException();
    }

    @Override
    public void commit()
    {
    }

    @Override
    public void commitCows()
    {
    }

    @Override
    public void rollback()
    {
    }

    @Override
    public boolean hasLocks()
    {
        return false;
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> getCowPropertyRemoveMap( Primitive primitive )
    {
        return null;
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> getCowPropertyAddMap( Primitive primitive )
    {
        return null;
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> getOrCreateCowPropertyAddMap( Primitive primitive )
    {
        throw new NotInTransactionException();
    }

    @Override
    public ArrayMap<Integer, DefinedProperty> getOrCreateCowPropertyRemoveMap( Primitive primitive )
    {
        throw new NotInTransactionException();
    }

    @Override
    public void deleteNode( long id )
    {
        throw new NotInTransactionException();
    }

    @Override
    public void deleteRelationship( long id )
    {
        throw new NotInTransactionException();
    }

    @Override
    public void createNode( long id )
    {
        throw new NotInTransactionException();
    }

    @Override
    public void createRelationship( long id )
    {
        throw new NotInTransactionException();
    }

    @Override
    public TransactionData getTransactionData()
    {
        throw new NotInTransactionException();
    }

    @Override
    public boolean nodeIsDeleted( long nodeId )
    {
        return false;
    }

    @Override
    public boolean relationshipIsDeleted( long relationshipId )
    {
        return false;
    }

    @Override
    public boolean hasChanges()
    {
        return false;
    }

    @Override
    public RemoteTxHook getTxHook()
    {
        return null;
    }

    @Override
    public TxIdGenerator getTxIdGenerator()
    {
        return null;
    }

    @Override
    public Set<Long> getCreatedNodes()
    {
        return emptySet();
    }

    @Override
    public Set<Long> getCreatedRelationships()
    {
        return emptySet();
    }

    @Override
    public Iterable<WritableTransactionState.CowNodeElement> getChangedNodes()
    {
        return Iterables.empty();
    }

    @Override
    public boolean isRemotelyInitialized()
    {
        return false;
    }

    @Override
    public void markAsRemotelyInitialized()
    {
    }

    @Override
    public ResourceHolder getNeoStoreTransaction()
    {
        return null;
    }

    @Override
    public void setNeoStoreTransaction( ResourceHolder neoStoreTransaction )
    {
    }
}
