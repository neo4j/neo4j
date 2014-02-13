/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.core.TransactionState;

public class TransactionalRelationshipLocker implements RelationshipLocker
{
    private TransactionState state;

    @Override
    public void getWriteLock( long relId )
    {
        state.acquireWriteLock( new LockableRelationship( relId ) );
    }

    private static class LockableRelationship implements Relationship
    {
        private final long id;

        LockableRelationship( long id )
        {
            this.id = id;
        }

        @Override
        public void delete()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Node getEndNode()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public long getId()
        {
            return this.id;
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Node[] getNodes()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Node getOtherNode( Node node )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Object getProperty( String key )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Node getStartNode()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public RelationshipType getType()
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public boolean isType( RelationshipType type )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public boolean hasProperty( String key )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public Object removeProperty( String key )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public void setProperty( String key, Object value )
        {
            throw new UnsupportedOperationException( "Lockable rel" );
        }

        @Override
        public boolean equals( Object o )
        {
            return o instanceof Relationship && this.getId() == ((Relationship) o).getId();
        }

        @Override
        public int hashCode()
        {
            return (int) ((id >>> 32) ^ id);
        }

        @Override
        public String toString()
        {
            return "Lockable relationship #" + this.getId();
        }
    }

    public void setTransactionState( TransactionState state )
    {
        this.state = state;
    }
}
