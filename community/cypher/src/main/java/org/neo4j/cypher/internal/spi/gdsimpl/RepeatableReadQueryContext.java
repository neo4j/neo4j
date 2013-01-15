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
package org.neo4j.cypher.internal.spi.gdsimpl;

import org.neo4j.cypher.internal.spi.QueryContext;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IterableWrapper;

/**
 * This QueryContext is responsible for taking read locks for all operations that read from the database.
 * <p/>
 * The close() method will then release all locks.
 */
public class RepeatableReadQueryContext implements QueryContext
{

    public interface Locker
    {
        void readLock( PropertyContainer pc );

        void releaseAllReadLocks();
    }

    private final QueryContext inner;
    private final Locker locker;
    private final Operations<Node> nodeOps;
    private final Operations<Relationship> relOps;

    public RepeatableReadQueryContext( QueryContext inner, Locker locker )
    {
        this.inner = inner;
        this.locker = locker;
        this.nodeOps = new LockingOperations<Node>( inner.nodeOps() );
        this.relOps = new LockingOperations<Relationship>( inner.relationshipOps() );
    }

    @Override
    public Operations<Node> nodeOps()
    {
        return nodeOps;
    }

    @Override
    public Operations<Relationship> relationshipOps()
    {
        return relOps;
    }

    @Override
    public Node createNode()
    {
        return inner.createNode();
    }

    @Override
    public Relationship createRelationship( Node start, Node end, String relType )
    {
        return inner.createRelationship( start, end, relType );
    }

    @Override
    public Iterable<Relationship> getRelationshipsFor( Node node, Direction dir, String... types )
    {
        locker.readLock( node );
        Iterable<Relationship> iter = inner.getRelationshipsFor( node, dir, types );
        return new LockingIterator( iter );
    }

    @Override
    public void addLabelsToNode(Node node, Iterable<Long> labelIds) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Long getOrCreateLabelId(String labelName) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close()
    {
        locker.releaseAllReadLocks();
    }

    private class LockingIterator extends IterableWrapper<Relationship, Relationship>
    {
        public LockingIterator( Iterable<Relationship> iterableToWrap )
        {
            super( iterableToWrap );
        }

        @Override
        protected Relationship underlyingObjectToObject( Relationship rel )
        {
            locker.readLock( rel );
            return rel;
        }
    }

    private class LockingOperations<T extends PropertyContainer> implements Operations<T>
    {
        private final Operations<T> inner;

        private LockingOperations( Operations<T> inner )
        {
            this.inner = inner;
        }

        @Override
        public void delete( T obj )
        {
            inner.delete( obj );
        }

        @Override
        public void setProperty( T obj, String propertyKey, Object value )
        {
            inner.setProperty( obj, propertyKey, value );
        }

        @Override
        public void removeProperty( T obj, String propertyKey )
        {
            inner.removeProperty( obj, propertyKey );
        }

        @Override
        public Object getProperty( T obj, String propertyKey )
        {
            locker.readLock( obj );
            return inner.getProperty( obj, propertyKey );
        }

        @Override
        public boolean hasProperty( T obj, String propertyKey )
        {
            locker.readLock( obj );
            return inner.hasProperty( obj, propertyKey );
        }

        @Override
        public Iterable<String> propertyKeys( T obj )
        {
            locker.readLock( obj );
            return inner.propertyKeys( obj );
        }

        @Override
        public T getById( long id )
        {
            T obj = inner.getById( id );
            locker.readLock( obj );
            return obj;
        }
    }
}
