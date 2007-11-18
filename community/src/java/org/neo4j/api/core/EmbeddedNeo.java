package org.neo4j.api.core;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.shell.NeoShellServer;

/**
 * The main Neo factory, with functionality to start and shutdown Neo, create
 * and get nodes and define valid relationship types. This class is typically
 * used in the outer loop in a Neo-enabled application, for example as follows:
 * <pre><code>
 * NeoService neo = new EmbeddedNeo( MyRelationshipTypes.class, "var/neo" );
 * // ... use neo
 * neo.shutdown();
 * </code></pre>
 * Neo is started when this class is instantiated. It provides operations to
 * {@link #createNode() create notes}, {@link #getNodeById(long) get nodes
 * given an id}, get the {@link #getReferenceNode() reference node} and
 * ultimately {@link #shutdown() shutdown Neo}. Typically, once instantiated
 * the reference to EmbeddedNeo is stored away in a service registry or in
 * a singleton instance.
 * <p>
 * Please note that after startup (i.e. constructor invocation), all operations
 * that read or write to the node space must be invoked in a {@link Transaction
 * transactional context}.
 */
public final class EmbeddedNeo implements NeoService
{
	private static Logger log = Logger.getLogger( EmbeddedNeo.class.getName() );
	private NeoShellServer shellServer;
	
	public EmbeddedNeo( String storeDir )
	{
		// TODO: implement
	}
	
	/**
	 * Creates an embedded neo instance with a given set of relationship types.
	 * It will use the store specified by <code>storeDir</code>, which will 
	 * be created if it doesn't already exist.
	 * @param validRelationshipTypesEnum an enum class containing your
	 * relationship types
	 * @param storeDir the store directory for the neo db files
 	 * @throws NullPointerException if clazz is <code>null</code>
 	 * @throws IllegalArgumentException if clazz not an enum
	 */
	public EmbeddedNeo( Class<? extends RelationshipType>
		validRelationshipTypesEnum, String storeDir )
	{
		this.shellServer = null;
		NeoJvmInstance.start( validRelationshipTypesEnum, storeDir, true );
	}
	
	// TODO: fix
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
	// TODO: Explain this concept
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

	/* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#registerEnumRelationshipTypes(java.lang.Class)
     */
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
     
    /* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#createAndRegisterRelationshipType(java.lang.String)
     */
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
    
    /* (non-Javadoc)
     * @see org.neo4j.api.core.NeoService#registerRelationshipTypes(java.lang.Iterable)
     */
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
