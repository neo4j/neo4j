package org.neo4j.impl.core;

import org.neo4j.impl.persistence.PersistenceMetadata;

public interface RelationshipTypeOperationEventData extends PersistenceMetadata
{
	public int getId();
	public String getName();
}
