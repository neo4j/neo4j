package org.neo4j.impl.batchinsert;

import org.neo4j.api.core.RelationshipType;

public class SimpleRelationship
{
    private final int id;
    private final int startNodeId;
    private final int endNodeId;
    private final RelationshipType type;

    SimpleRelationship( int id, int startNodeId, int endNodeId,
        RelationshipType type )
    {
        this.id = id;
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.type = type;
    }

    public long getId()
    {
        return id & 0xFFFFFFFFL;
    }

    public long getStartNode()
    {
        return startNodeId & 0xFFFFFFFFL;
    }

    public long getEndNode()
    {
        return endNodeId & 0xFFFFFFFFL;
    }

    public RelationshipType getType()
    {
        return type;
    }
}
