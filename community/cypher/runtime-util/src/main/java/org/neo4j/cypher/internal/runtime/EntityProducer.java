package org.neo4j.cypher.internal.runtime;

import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

public interface EntityProducer
{
    NodeValue nodeById( long id );

    RelationshipValue relationshipById( long id );
}
