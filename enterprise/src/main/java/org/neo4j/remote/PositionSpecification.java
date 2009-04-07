package org.neo4j.remote;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;

public final class PositionSpecification implements EncodedObject
{
    private static final long serialVersionUID = 1L;
    final int depth;
    final int returned;
    final long relationship;
    final long node;
    private final String type;
    private final long other;
    private final boolean startAtOther;

    PositionSpecification( int depth, int returned, Relationship last,
        Node current )
    {
        this.depth = depth;
        this.returned = returned;
        this.relationship = last.getId();
        this.node = current.getId();
        this.other = last.getOtherNode( current ).getId();
        this.type = last.getType().name();
        this.startAtOther = last.getEndNode().equals( current );
    }

    RelationshipSpecification relationshipSpec()
    {
        if ( startAtOther )
        {
            return new RelationshipSpecification( relationship, type, other,
                node );
        }
        else
        {
            return new RelationshipSpecification( relationship, type, node,
                other );
        }
    }
}
