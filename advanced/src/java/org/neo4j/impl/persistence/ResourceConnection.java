package org.neo4j.impl.persistence;


import javax.transaction.xa.XAResource;

import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;

/**
 * A connection to a {@link PersistenceSource}. <CODE>ResourceConnection</CODE>
 * contains operations to retrieve the {@link javax.transaction.xa.XAResource}
 * for this connection and to close the connection, optionally returning it to
 * a connection pool.
 * <P>
 * The most important method in a <CODE>ResourceConnection</CODE> --
 * the accessor for the actual connection -- is notably lacking from this
 * interface definition. It is provided by the classes that implement
 * <CODE>ResourceConnection</CODE>. For example, an
 * <CODE>SqlResourceConnection</CODE> provides a method that returns
 * a {@link javax.sql.XAConnection}, an <CODE>LDAPResourceConnection</CODE>
 * provides a method that returns whatever custom LDAP API connection it
 * uses, etc.
 * <P>
 * This interface definition does not imply any thread safety. It is custom
 * for clients to synchronize all resource operations on the
 * <CODE>ResourceConnection</CODE> instance.
 */
public interface ResourceConnection
{
	/**
	 * Returns the {@link javax.transaction.xa.XAResource} that represents
	 * this connection.
	 * @return the <CODE>XAResource</CODE> for this connection
	 */	
	public XAResource getXAResource();
	
	/**
	 * <CODE>destroy()</CODE> is invoked before this connection will
	 * be disposed of by the {@link ResourceBroker}. This method should
	 * perform any {@link PersistenceSource}-specific cleanups, which
	 * generally means calling things like
	 * {@link java.sql.Connection#close}.
	 * @throws ConnectionDestructionFailedException if the
	 * <CODE>PersistenceSource</CODE> failed to destroy the connection
	 */	
	public void destroy() throws ConnectionDestructionFailedException;
	
	public Object performOperation( PersistenceManager.Operation operation, 
		Object param ) throws PersistenceException;
	
	public void performUpdate( Event event, EventData data ) 
		throws PersistenceUpdateFailedException;
}
