package org.neo4j.impl.event;

/**
 * This typesafe enum [Bloch02] represents an event in the kernel.
 */
public class Event
{
	private String name = null;
	
	// Test event for junit tests.
	public static final Event TEST_EVENT = new Event( "TEST_EVENT" );
	
	// Lifecycle events
	public static final Event KERNEL_SHUTDOWN_REQUEST =
		new Event( "KERNEL_SHUTDOWN_REQUEST" );
	public static final Event KERNEL_SHUTDOWN_STARTED = 
		new Event( "KERNEL_SHUTDOWN_STARTED" ); // only sent proactively
	public static final Event KERNEL_STARTUP_COMPLETED =
		new Event( "KERNEL_STARTUP_COMPLETED" );
	public static final Event KERNEL_FREEZE_REQUEST =
		new Event( "KERNEL_FREEZE_REQUEST" );
	public static final Event KERNEL_THAW_REQUEST =
		new Event( "KERNEL_THAW_REQUEST" );
	
	// Neo related events
	public static final Event NODE_CREATE = 
		new Event( "NODE_CREATE" );
	public static final Event NODE_DELETE = 
		new Event( "NODE_DELETE" );
	public static final Event NODE_ADD_PROPERTY = 
		new Event( "NODE_ADD_PROPERTY" );
	public static final Event NODE_REMOVE_PROPERTY = 
		new Event( "NODE_REMOVE_PROPERTY" );
	public static final Event NODE_CHANGE_PROPERTY = 
		new Event( "NODE_CHANGE_PROPERTY" );
	public static final Event NODE_GET_PROPERTY = 
		new Event( "NODE_GET_PROPERTY" );
	public static final Event RELATIONSHIP_CREATE =
		new Event( "RELATIONSHIP_CREATE" );
	public static final Event RELATIONSHIP_DELETE = 
		new Event( "RELATIONSHIP_DELETE" );
	public static final Event RELATIONSHIP_ADD_PROPERTY = 
		new Event( "RELATIONSHIP_ADD_PROPERTY" );
	public static final Event RELATIONSHIP_REMOVE_PROPERTY = 
		new Event( "RELATIONSHIP_REMOVE_PROPERTY" );
	public static final Event RELATIONSHIP_CHANGE_PROPERTY = 
		new Event( "RELATIONSHIP_CHANGE_PROPERTY" );
	public static final Event RELATIONSHIP_GET_PROPERTY = 
		new Event( "RELATIONSHIP_GET_PROPERTY" );
	public static final Event RELATIONSHIPTYPE_CREATE = 
		new Event( "RELATIONSHIPTYPE_CREATE" );
	
	// Persistence related events
	public static final Event DATA_SOURCE_ADDED =
		new Event( "DATA_SOURCE_ADDED" );
	public static final Event DATA_SOURCE_REMOVED =
		new Event( "DATA_SOURCE_REMOVED" );

	// kernel principal and user related events
	public static final Event USER_CREATE = new Event( "USER_CREATE" );
	public static final Event USER_DELETE = new Event( "USER_DELETE" );
	public static final Event USER_SET_PASSWORD = 
		new Event( "USER_SET_PASSWORD" );
	
	// Transaction related events
	public static final Event TX_BEGIN = new Event( "TX_BEGIN" );
	public static final Event TX_ROLLBACK = new Event( "TX_ROLLBACK" );
	public static final Event TX_COMMIT = new Event( "TX_COMMIT" );
	
	public static final Event TX_IMMEDIATE_BEGIN =
		new Event( "TX_IMMEDIATE_BEGIN" );
	public static final Event TX_IMMEDIATE_ROLLBACK =
		new Event( "TX_IMMEDIATE_ROLLBACK" );
	public static final Event TX_IMMEDIATE_COMMIT =
		new Event( "TX_IMMEDIATE_COMMIT" );
	
	protected Event( String name )
	{
		this.name = name;
	}

	/**
	 * To string method.
	 *
	 * @return a string representation of this object
	 */
	@Override
	public String toString()
	{
		return name;
	}
}