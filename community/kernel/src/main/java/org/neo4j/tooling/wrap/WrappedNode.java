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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

public abstract class WrappedNode<G extends WrappedGraphDatabase> extends WrappedEntity<G, Node> implements Node
{
    protected WrappedNode( G graphdb )
    {
        super( graphdb );
    }

    @Override
    public long getId()
    {
        return actual().getId();
    }

    @Override
    public void delete()
    {
        actual().delete();
    }

    @Override
    public Iterable<Relationship> getRelationships()
    {
        return graphdb.relationships( actual().getRelationships() );
    }

    @Override
    public boolean hasRelationship()
    {
        return actual().hasRelationship();
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType... types )
    {
        return graphdb.relationships( actual().getRelationships( types ) );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
    {
        return graphdb.relationships( actual().getRelationships( direction, types ) );
    }

    @Override
    public boolean hasRelationship( RelationshipType... types )
    {
        return actual().hasRelationship( types );
    }

    @Override
    public boolean hasRelationship( Direction direction, RelationshipType... types )
    {
        return actual().hasRelationship( direction, types );
    }

    @Override
    public Iterable<Relationship> getRelationships( Direction dir )
    {
        return graphdb.relationships( actual().getRelationships( dir ) );
    }

    @Override
    public boolean hasRelationship( Direction dir )
    {
        return actual().hasRelationship( dir );
    }

    @Override
    public Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )
    {
        return graphdb.relationships( actual().getRelationships( type, dir ) );
    }

    @Override
    public boolean hasRelationship( RelationshipType type, Direction dir )
    {
        return actual().hasRelationship( type, dir );
    }

    @Override
    public Relationship getSingleRelationship( RelationshipType type, Direction dir )
    {
        Relationship rel = actual().getSingleRelationship( type, dir );
        if ( rel == null ) return null;
        return graphdb.relationship( rel, false );
    }

    @Override
    public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
    {
        return graphdb.relationship( actual().createRelationshipTo( unwrap( otherNode ), type ), true );
    }

    @Override
    public Traverser traverse( Order traversalOrder, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType, Direction direction )
    {
        WrappedTraverser.Evaluator evaluator = new WrappedTraverser.Evaluator( graphdb, stopEvaluator,
                returnableEvaluator );
        return new WrappedTraverser( graphdb, actual().traverse( traversalOrder, evaluator, evaluator, relationshipType,
                direction ) );
    }

    @Override
    public Traverser traverse( Order traversalOrder, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, RelationshipType firstRelationshipType, Direction firstDirection,
            RelationshipType secondRelationshipType, Direction secondDirection )
    {
        WrappedTraverser.Evaluator evaluator = new WrappedTraverser.Evaluator( graphdb, stopEvaluator,
                returnableEvaluator );
        return new WrappedTraverser( graphdb, actual().traverse( traversalOrder, evaluator, evaluator,
                firstRelationshipType, firstDirection, secondRelationshipType, secondDirection ) );
    }

    @Override
    public Traverser traverse( Order traversalOrder, StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator, Object... relationshipTypesAndDirections )
    {
        WrappedTraverser.Evaluator evaluator = new WrappedTraverser.Evaluator( graphdb, stopEvaluator,
                returnableEvaluator );
        return new WrappedTraverser( graphdb, actual().traverse( traversalOrder, evaluator, evaluator,
                relationshipTypesAndDirections ) );
    }
}
