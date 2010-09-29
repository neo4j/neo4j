package org.neo4j.index.impl.lucene;

import org.neo4j.graphdb.Relationship;

class RelationshipId
{
    final long id;
    final long startNode;
    final long endNode;

    RelationshipId( long id, long startNode, long endNode )
    {
        this.id = id;
        this.startNode = startNode;
        this.endNode = endNode;
    }
    
    public static RelationshipId of( Relationship rel )
    {
        return new RelationshipId( rel.getId(), rel.getStartNode().getId(), rel.getEndNode().getId() );
    }
    
    @Override
    public boolean equals( Object obj )
    {
        return ((RelationshipId) obj).id == id;
    }
    
    @Override
    public int hashCode()
    {
        return (int) id;
    }
}
