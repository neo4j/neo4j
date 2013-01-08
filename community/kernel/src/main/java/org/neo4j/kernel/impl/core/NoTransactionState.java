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
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.impl.core.WritableTransactionState.PrimitiveElement;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;

public class NoTransactionState implements TransactionState
{
    protected final PropertyIndex[] EMPTY_PROPERTY_INDEX_ARRAY = new PropertyIndex[0];

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
    public void dumpLocks()
    {
    }

    @Override
    public ArrayMap<Integer, PropertyData> getCowPropertyRemoveMap( Primitive primitive )
    {
        return null;
    }

    @Override
    public ArrayMap<Integer, PropertyData> getCowPropertyAddMap( Primitive primitive )
    {
        return null;
    }

    @Override
    public PrimitiveElement getPrimitiveElement( boolean create )
    {
        return null;
    }

    @Override
    public ArrayMap<Integer, PropertyData> getOrCreateCowPropertyAddMap( Primitive primitive )
    {
        throw new NotInTransactionException();
    }

    @Override
    public ArrayMap<Integer, PropertyData> getOrCreateCowPropertyRemoveMap( Primitive primitive )
    {
        throw new NotInTransactionException();
    }

    @Override
    public void deletePrimitive( Primitive primitive )
    {
        throw new NotInTransactionException();
    }

    @Override
    public void removeNodeFromCache( long nodeId )
    {
    }

    @Override
    public void addRelationshipType( NameData type )
    {
    }

    @Override
    public void addPropertyIndex( NameData index )
    {
    }

    @Override
    public void removeRelationshipFromCache( long id )
    {
    }

    @Override
    public void removeRelationshipTypeFromCache( int id )
    {
    }

    @Override
    public void removeGraphPropertiesFromCache()
    {
    }

    @Override
    public void clearCache()
    {
    }

    @Override
    public TransactionData getTransactionData()
    {
        throw new NotInTransactionException();
    }

    @Override
    public void addPropertyIndex( PropertyIndex index )
    {
        throw new NotInTransactionException();
    }

    @Override
    public PropertyIndex getPropertyIndex( String key )
    {
        return null;
    }

    @Override
    public PropertyIndex getPropertyIndex( int keyId )
    {
        return null;
    }
    
    @Override
    public PropertyIndex[] getAddedPropertyIndexes()
    {
        return EMPTY_PROPERTY_INDEX_ARRAY;
    }

    @Override
    public boolean isDeleted( Node node )
    {
        return false;
    }

    @Override
    public boolean isDeleted( Relationship relationship )
    {
        return false;
    }

    @Override
    public boolean hasChanges()
    {
        return false;
    }
    
    @Override
    public void setRollbackOnly()
    {
    }
    
    @Override
    public TxHook getTxHook()
    {
        return null;
    }
    
    @Override
    public TxIdGenerator getTxIdGenerator()
    {
        return null;
    }
}
