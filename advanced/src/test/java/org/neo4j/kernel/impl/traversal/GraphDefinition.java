package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public interface GraphDefinition
{
    Node create( GraphDatabaseService graphdb );
}
