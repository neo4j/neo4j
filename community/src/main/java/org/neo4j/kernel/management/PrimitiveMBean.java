package org.neo4j.kernel.management;

public interface PrimitiveMBean
{
    final String NAME = "Primitive count";

    long getNumberOfNodeIdsInUse();

    long getNumberOfRelationshipIdsInUse();

    long getNumberOfRelationshipTypeIdsInUse();

    long getNumberOfPropertyIdsInUse();
}
