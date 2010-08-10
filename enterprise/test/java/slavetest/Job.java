package slavetest;

import org.neo4j.graphdb.GraphDatabaseService;

public interface Job<T>
{
    T execute( GraphDatabaseService db );
}
