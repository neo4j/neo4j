package slavetest;

import java.io.Serializable;
import java.rmi.RemoteException;

import org.neo4j.graphdb.GraphDatabaseService;

public interface Job<T> extends Serializable
{
    T execute( GraphDatabaseService db ) throws RemoteException;
}
