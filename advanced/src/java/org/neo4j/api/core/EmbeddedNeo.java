package org.neo4j.api.core;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.shell.NeoShellServer;

/**
 * An implementation of {@link NeoService} that is used to embed Neo
 * in an application. There are two ways to instantiate this class:
 * <ul>
 * <li>A {@link #EmbeddedNeo(String) single argument constructor} that takes
 * a path to a directory where Neo will store its data files. This will start
 * Neo with an empty set of valid relationship types, and the client must
 * subsequently invoke {@link NeoService#registerRelationshipType
 * registerRelationshipType} to declare the valid relationship types.
 * <li> A {@link #EmbeddedNeo(Class, String) two argument constructor}
 * that in addition to the store file location takes an enum class that
 * implements {@link RelationshipType RelationshipType} as an initial set of
 * valid relationship types. 
 * </ul>
 * 
 * The second case is more common, since most applications deal with a static
 * set of relationship types and then prefer to gather them in an enum to
 * provide compile-time safety (see the {@link RelationshipType
 * RelationshipType docs} for more information). Here's how you would do that:
 * <code><pre> enum MyRelationshipTypes implements RelationshipType
 * {
 *     CONTAINED_IN, KNOWS
 * }
 * 
 * // ... later in some main() method
 * NeoService neo = new EmbeddedNeo( MyRelationshipTypes.class, "var/neo" );
 * // ... use neo
 * neo.shutdown();</pre>
 * </code>
 * (Please note that the constructor signatures may change slightly in upcoming
 * 1.0-betas.)
 * <p>
 * Typically, once instantiated the reference to NeoService is stored away in a
 * service registry or in a singleton instance.
 * <p>
 * For more information, see {@link NeoService}.
 */
public final class EmbeddedNeo implements NeoService
{
	private static Logger log = Logger.getLogger( EmbeddedNeo.class.getName() );
	private NeoShellServer shellServer;
	
	/**
	 * Creates an embedded {@link NeoService} with an empty set of valid
	 * relationship types. It will use the store located in
	 * <code>storeDir</code>, which will be created if it doesn't already exist.
	 * With this constructor, the client is expected to {@link
	 * #registerRelationshipType register relationship types} in order to set
	 * up a valid set of relationship types.
	 * @param storeDir the store directory for the neo db files
	 */
	public EmbeddedNeo( String storeDir )
	{
		this.shellServer = null;
		NeoJvmInstance.start( null, storeDir, true );
	}
	
	/**
	 * Creates an embedded {@link NeoService} with a set of valid relationship
	 * types defined by the supplied enum class. It will use the store located in
	 * <code>storeDir</code>, which will be created if it doesn't already exist.
	 * @param validRelationshipTypesEnum an enum class containing your
	 * relationship types, as described in the documentation of
	 * {@link RelationshipType}
	 * @param storeDir the store directory for the neo db files
 	 * @throws NullPointerException if validRelationshipTypesEnum is
 	 * <code>null</code>
 	 * @throws IllegalArgumentException if validRelationshipTypesEnum is not an
 	 * enum that implements <code>RelationshipType</code>
	 */
	public EmbeddedNeo( Class<? extends RelationshipType>
		validRelationshipTypesEnum, String storeDir )
	{
		this.shellServer = null;
		NeoJvmInstance.start( validRelationshipTypesEnum, storeDir, true );
	}
	
//	public EmbeddedNeo( String dir, RelationshipType[] relationshipTypes, 
//		Map<String,String> params )
//	{
//		this.shellServer = null;
//		NeoJvmInstance.start( null, dir, true, params );
//	}
	
 	// private accessor for the remote shell (started with enableRemoteShell())
	private NeoShellServer getShellServer()
	{
		return this.shellServer;
	}
	
	/* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#createNode()
     */
	public Node createNode()
	{
		return NodeManager.getManager().createNode();
	}
	
	/* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getNodeById(long)
     */
	public Node getNodeById( long id )
	{
		return NodeManager.getManager().getNodeById( (int) id );
	}
	
	/* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getReferenceNode()
     */
	public Node getReferenceNode()
	{
		return NodeManager.getManager().getReferenceNode();
	}
	
	/* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#shutdown()
     */
	public void shutdown()
	{
		if ( getShellServer() != null )
		{
			try
			{
				getShellServer().shutdown();
			}
			catch ( Throwable t )
			{
				log.warning( "Error shutting down shell server: " + t );
			}
		}
		NeoJvmInstance.shutdown();
	}
	
	/* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#enableRemoteShell()
     */
	public boolean enableRemoteShell()
	{
		return this.enableRemoteShell( null );
	}
	
	/* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#enableRemoteShell(java.util.Map)
     */
	public boolean enableRemoteShell( Map<String, Serializable>
		initialProperties )
	{
		try
		{
            if ( initialProperties == null )
            {
                initialProperties = Collections.emptyMap();
            }
			if ( shellDependencyAvailable() )
			{
				this.shellServer = new NeoShellServer( this );
				Object port = initialProperties.get( "port" );
				Object name = initialProperties.get( "name" );
				this.shellServer.makeRemotelyAvailable( 
					port != null ? ( Integer ) port : 1337,
					name != null ? ( String ) name : "shell" );
				return true;
			}
			else
			{
				log.info( "Shell library not available. Neo shell not " +
					"started. Please add the Neo4j shell jar to the " +
					"classpath." );
				return false;
			}
		}
		catch ( RemoteException e )
		{
			throw new IllegalStateException( "Can't start remote neo shell: " +
				e );
		}
	}
	
	private boolean shellDependencyAvailable()
	{
		try
		{
			Class.forName( "org.neo4j.util.shell.ShellServer" );
			return true;
		}
		catch ( Throwable t )
		{
			return false;
		}
	}

    // TODO: doc: kept for backwards compat
	// TODO: prio 1
	public void registerEnumRelationshipTypes( 
		Class<? extends RelationshipType> relationshipTypes )
	{
		NeoJvmInstance.addEnumRelationshipTypes( relationshipTypes );
	}

    /* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getRelationshipTypes()
     */
    public Iterable<RelationshipType> getRelationshipTypes()
    {
    	return NeoJvmInstance.getRelationshipTypes();
    }
    
    /* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#getRelationshipType(java.lang.String)
     */
    public RelationshipType getRelationshipType( String name )
    {
		return NeoJvmInstance.getRelationshipTypeByName( name );
    }
    
    /* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#hasRelationshipType(java.lang.String)
     */
    public boolean hasRelationshipType( String name )
    {
    	return NeoJvmInstance.hasRelationshipType( name );
    	
    }
     
    // TODO: doc: kept for backwards compat
    // TODO: prio 1
    public RelationshipType createAndRegisterRelationshipType( String name )
    {
    	return NeoJvmInstance.registerRelationshipType( name, true );
    }
    
    /* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#registerRelationshipType(java.lang.String)
     */
    public RelationshipType registerRelationshipType( String name )
    {
    	return NeoJvmInstance.registerRelationshipType( name, false );
    }

    // TODO: doc: kept for backwards compat
    // TODO: prio 1
    public void registerRelationshipTypes( Iterable<RelationshipType> types )
    {
    	for ( RelationshipType type : types )
    	{
    		NeoJvmInstance.registerRelationshipType( type.name(), true );
    	}
    }
    
    /* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#registerRelationshipTypes(org.neo4j.api.core.RelationshipType[])
     */
    public void registerRelationshipTypes( RelationshipType[] types )
    {
       	for ( RelationshipType type : types )
    	{
    		NeoJvmInstance.registerRelationshipType( type.name(), true );
    	}
    }
}
