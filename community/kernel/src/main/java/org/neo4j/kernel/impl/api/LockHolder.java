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
package org.neo4j.kernel.impl.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.transaction.Transaction;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;

public class LockHolder
{
    private final LockManager lockManager;
    private final Transaction tx;
    private final List<Releasable> locks = new ArrayList<Releasable>();

    public LockHolder( LockManager lockManager, Transaction tx )
    {
        if ( tx == null )
        {
            throw new RuntimeException( "Cannot initialize lock holder without a transaction, got null." );
        }
        this.lockManager = lockManager;
        this.tx = tx;
    }

    public void acquireNodeReadLock( long nodeId )
    {
        NodeLock resource = new NodeLock( LockType.READ, nodeId );
        lockManager.getReadLock( resource, tx );
        locks.add( resource );
    }

    public void acquireNodeWriteLock( long nodeId )
    {
        NodeLock resource = new NodeLock( LockType.WRITE, nodeId );
        lockManager.getWriteLock( resource, tx );
        locks.add( resource );
    }
    
    public void acquireSchemaReadLock()
    {
        SchemaLock resource = new SchemaLock( LockType.READ );
        lockManager.getReadLock( resource, tx );
        locks.add( resource );
    }

    public void acquireSchemaWriteLock()
    {
        SchemaLock resource = new SchemaLock( LockType.WRITE );
        lockManager.getWriteLock( resource, tx );
        locks.add( resource );
    }
    
    public void releaseLocks()
    {
        Collection<Releasable> releaseFailures = null;
        Exception releaseException = null;
        for ( Releasable lockElement : locks )
        {
            try
            {
                lockElement.release();
            }
            catch ( Exception e )
            {
                releaseException = e;
                if ( releaseFailures == null )
                    releaseFailures = new ArrayList<Releasable>();
                releaseFailures.add( lockElement );
            }
        }
        
        if ( releaseException != null )
        {
            throw new RuntimeException( "Unable to release locks: " + releaseFailures + ".", releaseException );
        }
    }
    
    private abstract class Releasable
    {
        private final LockType lockType;
        
        public Releasable( LockType lockType )
        {
            this.lockType = lockType;
        }
        
        public void release()
        {
            lockType.release( lockManager, this, tx );
        }
    }

    // Have them be releasable also since they are internal and will save the
    // amount of garbage produced
    private class NodeLock extends Releasable implements Node
    {
        private final long id;

        public NodeLock( LockType lockType, long id )
        {
            super( lockType );
            this.id = id;
        }
        
        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof Node) )
            {
                return false;
            }
            return this.getId() == ((Node) o).getId();
        }

        @Override
        public int hashCode()
        {
            return (int) (( id >>> 32 ) ^ id );
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public void delete()
        {
            throw unsupportedOperation();
        }

        @Override
        public Iterable<Relationship> getRelationships()
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean hasRelationship()
        {
            throw unsupportedOperation();
        }

        @Override
        public Iterable<Relationship> getRelationships( RelationshipType... types )
        {
            throw unsupportedOperation();
        }

        @Override
        public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean hasRelationship( RelationshipType... types )
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean hasRelationship( Direction direction, RelationshipType... types )
        {
            throw unsupportedOperation();
        }

        @Override
        public Iterable<Relationship> getRelationships( Direction dir )
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean hasRelationship( Direction dir )
        {
            throw unsupportedOperation();
        }

        @Override
        public Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean hasRelationship( RelationshipType type, Direction dir )
        {
            throw unsupportedOperation();
        }

        @Override
        public Relationship getSingleRelationship( RelationshipType type, Direction dir )
        {
            throw unsupportedOperation();
        }

        @Override
        public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
        {
            throw unsupportedOperation();
        }

        @Override
        public Traverser traverse( Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator
                returnableEvaluator, RelationshipType relationshipType, Direction direction )
        {
            throw unsupportedOperation();
        }

        @Override
        public Traverser traverse( Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator
                returnableEvaluator, RelationshipType firstRelationshipType, Direction firstDirection,
                                   RelationshipType secondRelationshipType, Direction secondDirection )
        {
            throw unsupportedOperation();
        }

        @Override
        public Traverser traverse( Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator
                returnableEvaluator, Object... relationshipTypesAndDirections )
        {
            throw unsupportedOperation();
        }

        @Override
        public void addLabel( Label label )
        {
            throw unsupportedOperation();
        }

        @Override
        public void removeLabel( Label label )
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean hasLabel( Label label )
        {
            throw unsupportedOperation();
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean hasProperty( String key )
        {
            throw unsupportedOperation();
        }

        @Override
        public Object getProperty( String key )
        {
            throw unsupportedOperation();
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            throw unsupportedOperation();
        }

        @Override
        public void setProperty( String key, Object value )
        {
            throw unsupportedOperation();
        }

        @Override
        public Object removeProperty( String key )
        {
            throw unsupportedOperation();
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            throw unsupportedOperation();
        }

        @Override
        public Iterable<Object> getPropertyValues()
        {
            throw unsupportedOperation();
        }

        @Override
        public Iterable<Label> getLabels()
        {
            throw unsupportedOperation();
        }
        
        private UnsupportedOperationException unsupportedOperation()
        {
            return new UnsupportedOperationException( "NodeLock does not support this operation." );
        }
    }

    class SchemaLock extends Releasable
    {
        private final org.neo4j.kernel.impl.core.SchemaLock actual = new org.neo4j.kernel.impl.core.SchemaLock();
        
        public SchemaLock( LockType lockType )
        {
            super( lockType );
        }

        @Override
        public int hashCode()
        {
            return actual.hashCode();
        }
        
        @Override
        public boolean equals( Object obj )
        {
            return actual.equals( obj );
        }
    }
}
