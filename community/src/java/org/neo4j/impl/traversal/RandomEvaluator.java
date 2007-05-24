package org.neo4j.impl.traversal;


import org.neo4j.api.core.RelationshipType;

public interface RandomEvaluator
{
	public boolean shouldRandomize( TraversalPositionImpl position, 
		RelationshipType type );
}
