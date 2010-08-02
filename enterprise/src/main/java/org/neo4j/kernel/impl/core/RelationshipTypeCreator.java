package org.neo4j.kernel.impl.core;

import javax.transaction.TransactionManager;

import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

public interface RelationshipTypeCreator
{
    int create( TransactionManager txManager, EntityIdGenerator idGenerator,
            PersistenceManager persistence, String name );
}
