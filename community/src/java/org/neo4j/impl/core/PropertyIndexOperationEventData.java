package org.neo4j.impl.core;

import org.neo4j.impl.persistence.PersistenceMetadata;

public interface PropertyIndexOperationEventData extends PersistenceMetadata
{
	public PropertyIndex getIndex();
}