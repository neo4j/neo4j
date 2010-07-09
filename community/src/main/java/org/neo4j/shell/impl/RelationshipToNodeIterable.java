package org.neo4j.shell.impl;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IterableWrapper;

public class RelationshipToNodeIterable extends IterableWrapper<Node, Relationship>
{
    private final Node fromNode;

    public RelationshipToNodeIterable( Iterable<Relationship> iterableToWrap, Node fromNode )
    {
        super( iterableToWrap );
        this.fromNode = fromNode;
    }

    @Override
    protected Node underlyingObjectToObject( Relationship rel )
    {
        return rel.getOtherNode( fromNode );
    }
    
    public static Iterable<Node> wrap( Iterable<Relationship> relationships, Node fromNode )
    {
        return new RelationshipToNodeIterable( relationships, fromNode );
    }
}
