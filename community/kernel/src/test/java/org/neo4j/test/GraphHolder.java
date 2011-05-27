package org.neo4j.test;

import org.neo4j.graphdb.GraphDatabaseService;

public interface GraphHolder
{
    GraphDatabaseService graphdb();
}
