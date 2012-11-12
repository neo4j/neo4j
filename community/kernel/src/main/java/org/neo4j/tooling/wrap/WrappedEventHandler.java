/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.tooling.wrap;

import static org.neo4j.tooling.wrap.WrappedEntity.unwrap;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.helpers.collection.IterableWrapper;

class WrappedEventHandler<T> extends WrappedObject<TransactionEventHandler<T>> implements TransactionEventHandler<T>
{
    WrappedEventHandler( WrappedGraphDatabase graphdb, TransactionEventHandler<T> handler )
    {
        super( graphdb, handler );
    }

    @Override
    public T beforeCommit( TransactionData data ) throws Exception
    {
        return wrapped.beforeCommit( new WrappedData( graphdb, data ) );
    }

    @Override
    public void afterCommit( TransactionData data, T state )
    {
        wrapped.afterCommit( new WrappedData( graphdb, data ), state );
    }

    @Override
    public void afterRollback( TransactionData data, T state )
    {
        wrapped.afterRollback( new WrappedData( graphdb, data ), state );
    }

    private static class WrappedData extends WrappedObject<TransactionData> implements TransactionData
    {
        WrappedData( WrappedGraphDatabase graphdb, TransactionData data )
        {
            super( graphdb, data );
        }

        @Override
        public Iterable<Node> createdNodes()
        {
            return graphdb.nodes( wrapped.createdNodes() );
        }

        @Override
        public Iterable<Node> deletedNodes()
        {
            return graphdb.nodes( wrapped.deletedNodes() );
        }

        @Override
        public boolean isDeleted( Node node )
        {
            return wrapped.isDeleted( unwrap( node ) );
        }

        @Override
        public Iterable<PropertyEntry<Node>> assignedNodeProperties()
        {
            return new NodeProperties( graphdb, wrapped.assignedNodeProperties() );
        }

        @Override
        public Iterable<PropertyEntry<Node>> removedNodeProperties()
        {
            return new NodeProperties( graphdb, wrapped.removedNodeProperties() );
        }

        @Override
        public Iterable<Relationship> createdRelationships()
        {
            return graphdb.relationships( wrapped.createdRelationships() );
        }

        @Override
        public Iterable<Relationship> deletedRelationships()
        {
            return graphdb.relationships( wrapped.deletedRelationships() );
        }

        @Override
        public boolean isDeleted( Relationship relationship )
        {
            return wrapped.isDeleted( unwrap( relationship ) );
        }

        @Override
        public Iterable<PropertyEntry<Relationship>> assignedRelationshipProperties()
        {
            return new RelationshipProperties( graphdb, wrapped.assignedRelationshipProperties() );
        }

        @Override
        public Iterable<PropertyEntry<Relationship>> removedRelationshipProperties()
        {
            return new RelationshipProperties( graphdb, wrapped.removedRelationshipProperties() );
        }
    }

    private static class NodeProperties extends IterableWrapper<PropertyEntry<Node>, PropertyEntry<Node>>
    {
        private final WrappedGraphDatabase graphdb;

        NodeProperties( WrappedGraphDatabase graphdb, Iterable<PropertyEntry<Node>> iterable )
        {
            super( iterable );
            this.graphdb = graphdb;
        }

        @Override
        protected PropertyEntry<Node> underlyingObjectToObject( PropertyEntry<Node> object )
        {
            return new NodePropertyEntry( graphdb, object );
        }
    }

    private static class RelationshipProperties extends
            IterableWrapper<PropertyEntry<Relationship>, PropertyEntry<Relationship>>
    {
        private final WrappedGraphDatabase graphdb;

        RelationshipProperties( WrappedGraphDatabase graphdb, Iterable<PropertyEntry<Relationship>> iterable )
        {
            super( iterable );
            this.graphdb = graphdb;
        }

        @Override
        protected PropertyEntry<Relationship> underlyingObjectToObject( PropertyEntry<Relationship> object )
        {
            return new RelationshipPropertyEntry( graphdb, object );
        }
    }

    private static class NodePropertyEntry extends WrappedPropertyEntry<Node>
    {
        NodePropertyEntry( WrappedGraphDatabase graphdb, PropertyEntry<Node> entry )
        {
            super( graphdb, entry );
        }

        @Override
        public Node entity()
        {
            return graphdb.node( wrapped.entity(), false );
        }
    }

    private static class RelationshipPropertyEntry extends WrappedPropertyEntry<Relationship>
    {
        RelationshipPropertyEntry( WrappedGraphDatabase graphdb, PropertyEntry<Relationship> entry )
        {
            super( graphdb, entry );
        }

        @Override
        public Relationship entity()
        {
            return graphdb.relationship( wrapped.entity(), false );
        }
    }

    private static abstract class WrappedPropertyEntry<T extends PropertyContainer> extends
            WrappedObject<PropertyEntry<T>> implements PropertyEntry<T>
    {
        WrappedPropertyEntry( WrappedGraphDatabase graphdb, PropertyEntry<T> entry )
        {
            super( graphdb, entry );
        }

        @Override
        public String key()
        {
            return wrapped.key();
        }

        @Override
        public Object previouslyCommitedValue()
        {
            return wrapped.previouslyCommitedValue();
        }

        @Override
        public Object value()
        {
            return wrapped.value();
        }
    }
}
