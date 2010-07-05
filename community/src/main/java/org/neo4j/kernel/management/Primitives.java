package org.neo4j.kernel.management;

public interface Primitives
{
    final String NAME = "Primitive count";

    long getNumberOfNodeIdsInUse();

    long getNumberOfRelationshipIdsInUse();

    long getNumberOfRelationshipTypeIdsInUse();

    long getNumberOfPropertyIdsInUse();
}
