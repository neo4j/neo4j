package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Session extends Remote
{
	void set( String key, Serializable value ) throws RemoteException;
	
	Serializable get( String key ) throws RemoteException;
	
	Serializable remove( String key ) throws RemoteException;
	
	String[] keys() throws RemoteException;
}
