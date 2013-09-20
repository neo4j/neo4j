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
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.kernel.api.exceptions.ReleaseLocksFailedKernelException;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.SchemaLock;
import org.neo4j.kernel.impl.locking.IndexEntryLock;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;

public class LockHolderImpl implements LockHolder
{
    private final LockManager lockManager;
    private final Transaction tx;
    private final List<LockReleaseCallback> locks = new ArrayList<>();
    private final NodeManager nodeManager;

    public LockHolderImpl( LockManager lockManager, Transaction tx, NodeManager nodeManager )
    {
        this.lockManager = lockManager;
        this.tx = tx;

        // TODO Not happy about the NodeManager dependency. It's needed a.t.m. for making
        // equality comparison between GraphProperties instances. It should change.
        this.nodeManager = nodeManager;
    }

    @Override
    public void acquireNodeReadLock( long nodeId )
    {
        NodeLock resource = new NodeLock( nodeId );
        lockManager.getReadLock( resource, tx );
        locks.add( new LockReleaseCallback( LockType.READ, resource ) );
    }

    @Override
    public void acquireNodeWriteLock( long nodeId )
    {
        NodeLock resource = new NodeLock( nodeId );
        lockManager.getWriteLock( resource, tx );
        locks.add( new LockReleaseCallback( LockType.WRITE, resource ) );
    }

    @Override
    public void acquireRelationshipReadLock( long relationshipId )
    {
        RelationshipLock resource = new RelationshipLock( relationshipId );
        lockManager.getReadLock( resource, tx );
        locks.add( new LockReleaseCallback( LockType.READ, resource ) );
    }

    @Override
    public void acquireRelationshipWriteLock( long relationshipId )
    {
        RelationshipLock resource = new RelationshipLock( relationshipId );
        lockManager.getWriteLock( resource, tx );
        locks.add( new LockReleaseCallback( LockType.WRITE, resource ) );
    }

    @Override
    public void acquireGraphWriteLock()
    {
        GraphLock resource = new GraphLock();
        lockManager.getWriteLock( resource, tx );
        locks.add( new LockReleaseCallback( LockType.WRITE, resource ) );
    }

    @Override
    public void acquireSchemaReadLock()
    {
        SchemaLock resource = new SchemaLock();
        lockManager.getReadLock( resource, tx );
        locks.add( new LockReleaseCallback( LockType.READ, resource ) );
    }

    @Override
    public void acquireSchemaWriteLock()
    {
        SchemaLock resource = new SchemaLock();
        lockManager.getWriteLock( resource, tx );
        locks.add( new LockReleaseCallback( LockType.WRITE, resource ) );
    }

    @Override
    public void acquireIndexEntryReadLock( int labelId, int propertyKeyId, String propertyValue )
    {
        IndexEntryLock resource = new IndexEntryLock( labelId, propertyKeyId, propertyValue );
        lockManager.getReadLock( resource, tx );
        locks.add( new LockReleaseCallback( LockType.READ, resource ) );
    }

    @Override
    public void acquireIndexEntryWriteLock( int labelId, int propertyKeyId, String propertyValue )
    {
        IndexEntryLock resource = new IndexEntryLock( labelId, propertyKeyId, propertyValue );
        lockManager.getWriteLock( resource, tx );
        locks.add( new LockReleaseCallback( LockType.WRITE, resource ) );
    }

    @Override
    public void releaseLocks() throws ReleaseLocksFailedKernelException
    {
        Collection<LockReleaseCallback> releaseFailures = null;
        Exception releaseException = null;
        for ( LockReleaseCallback lockElement : locks )
        {
            try
            {
                lockElement.release();
            }
            catch ( Exception e )
            {
                releaseException = e;
                if ( releaseFailures == null )
                {
                    releaseFailures = new ArrayList<>();
                }
                releaseFailures.add( lockElement );
            }
        }

        if ( releaseException != null )
        {
            throw new ReleaseLocksFailedKernelException( "Unable to release locks: " + releaseFailures + ".",
                    releaseException );
        }
    }

    private final class LockReleaseCallback
    {
        private final LockType lockType;
        private final Object lock;

        public LockReleaseCallback( LockType lockType, Object lock )
        {
            this.lockType = lockType;
            this.lock = lock;
        }

        public void release()
        {
            lockType.release( lockManager, lock, tx );
        }

        @Override
        public String toString()
        {
            return String.format( "%s_LOCK(%s)", lockType.name(), lock );
        }
    }

    private abstract class EntityLock implements PropertyContainer
    {
        private final long id;

        public EntityLock( long id )
        {
            this.id = id;
        }

        @Override
        public String toString()
        {
            return String.format( "%s[id=%d]", getClass().getSimpleName(), id );
        }

        public long getId()
        {
            return id;
        }

        public void delete()
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
        @Deprecated
        public Iterable<Object> getPropertyValues()
        {
            throw unsupportedOperation();
        }

        @Override
        public int hashCode()
        {
            return (int) ((id >>> 32) ^ id);
        }

        protected UnsupportedOperationException unsupportedOperation()
        {
            return new UnsupportedOperationException( getClass().getSimpleName() +
                    " does not support this operation." );
        }
    }

    // Have them be releasable also since they are internal and will save the
    // amount of garbage produced
    @SuppressWarnings("deprecation")
    private class NodeLock extends EntityLock implements Node
    {
        public NodeLock( long id )
        {
            super( id );
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
        public ResourceIterable<Label> getLabels()
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean equals( Object o )
        {
            return o instanceof Node && this.getId() == ((Node) o).getId();
        }
        // NOTE hashCode is implemented in super
    }

    private class RelationshipLock extends EntityLock implements Relationship
    {
        public RelationshipLock( long id )
        {
            super( id );
        }

        @Override
        public Node getStartNode()
        {
            throw unsupportedOperation();
        }

        @Override
        public Node getEndNode()
        {
            throw unsupportedOperation();
        }

        @Override
        public Node getOtherNode( Node node )
        {
            throw unsupportedOperation();
        }

        @Override
        public Node[] getNodes()
        {
            throw unsupportedOperation();
        }

        @Override
        public RelationshipType getType()
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean isType( RelationshipType type )
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean equals( Object o )
        {
            return o instanceof Relationship && this.getId() == ((Relationship) o).getId();
        }
    }

    private class GraphLock extends EntityLock implements GraphProperties
    {
        public GraphLock()
        {
            super( -1 );
        }

        @Override
        public NodeManager getNodeManager()
        {
            return nodeManager;
        }

        @Override
        public boolean equals( Object o )
        {
            return o instanceof GraphProperties &&
                    this.getNodeManager().equals( ((GraphProperties) o).getNodeManager() );
        }
    }
}
