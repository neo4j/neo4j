package common;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

public interface GraphDefinition
{
    Node create( GraphDatabaseService graphdb );
}
