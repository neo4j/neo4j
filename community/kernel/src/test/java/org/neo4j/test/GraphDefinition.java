package org.neo4j.test;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public interface GraphDefinition
{
    Map<String, Node> create( GraphDatabaseService graphdb );
}
