package org.neo4j.graphdb.traversal;

public interface UniquenessFactory
{
    UniquenessFilter create( Object optionalParameter );
}
