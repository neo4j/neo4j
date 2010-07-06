package org.neo4j.kernel.impl.persistence;

public interface EntityIdGenerator
{
    int nextId( Class<?> clazz );

    long getHighestPossibleIdInUse( Class<?> clazz );

    long getNumberOfIdsInUse( Class<?> clazz );
}
