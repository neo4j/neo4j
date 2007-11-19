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
 * in an application. You typically instantiate it by invoking the
 * {@link #EmbeddedNeo(String) single argument constructor} that takes
 * a path to a directory where Neo will store its data files.
 * <p>
 * There's a {@link #EmbeddedNeo(Class, String) legacy constructor} which
 * was used in earlier versions to define valid {@link RelationshipType
 * relationship types}. Since version <code>1.0-b6</code>, relationship types
 * are {@link RelationshipType dynamically created} so it's now been marked
 * deprecated. The same goes for all the relationship type management
 * operations. Expect them to be removed in future releases.
 * <p>
 * For more information, see {@link NeoService}.
 */
public final class EmbeddedNeo implements NeoService
{
	private static Logger log = Logger.getLogger( EmbeddedNeo.class.getName() );
	private NeoShellServer shellServer;
	
	/**
	 * Creates an embedded {@link NeoService} with a store located in
	 * <code>storeDir</code>, which will be created if it doesn't already exist.
	 * @param storeDir the store directory for the neo db files
	 */
	public EmbeddedNeo( String storeDir )
	{
		this.shellServer = null;
		NeoJvmInstance.start( null, storeDir, true );
	}
	
	/**
	 * Creates an embedded {@link NeoService} that uses the store located in
	 * <code>storeDir</code>. This constructor is kept for backwards
	 * compatibility. It accepted an enum which defined a valid set of
	 * relationship types. Relationship types are now {@link RelationshipType
	 * dynamically created}, so this constructor is deprecated. Invoking it
	 * is identical to invoking <code>EmbeddedNeo(storeDir)</code>.
	 * @param validRelationshipTypesEnum an enum class containing your
	 * relationship types, as described in the documentation of
	 * {@link RelationshipType}
	 * @param storeDir the store directory for the neo db files
 	 * @throws NullPointerException if validRelationshipTypesEnum is
 	 * <code>null</code>
 	 * @throws IllegalArgumentException if validRelationshipTypesEnum is not an
 	 * enum that implements <code>RelationshipType</code>
     * @deprecated Not required now that relationship types are {@link
     * RelationshipType created dynamically}. Will be removed in next release.
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

    /**
     * Returns all relationship types in the underlying store. Relationship
     * types are added to the underlying store the first time they are used
     * in {@link Node#createRelationshipTo}.
     * @return all relationship types in the underlying store
     * @deprecated Might not be needed now that relationship types are {@link
     * RelationshipType created dynamically}.
     */
    public Iterable<RelationshipType> getRelationshipTypes()
    {
    	return NeoJvmInstance.getRelationshipTypes();
    }
    
    /**
     * Returns a relationship type with the same name if it exists in the
     * underlying store, or <code>null</code>. A new relationship type is
     * added to the underlying store the first time a relationship with the
     * new type is {@link Node#createRelationshipTo(Node, RelationshipType)
     * created}.
     * @param name the name of the relationship type
     * @return a relationship type with the given name, or <code>null</code>
     * if there's no such relationship type in the underlying store
     * @deprecated Might not be needed now that relationship types are {@link
     * RelationshipType created dynamically}.
     */
    public RelationshipType getRelationshipType( String name )
    {
		return NeoJvmInstance.getRelationshipTypeByName( name );
    }
    
	/**
     * @deprecated Not required now that relationship types are {@link
     * RelationshipType created dynamically}. Will be removed in next release.
	 */
    public boolean hasRelationshipType( String name )
    {
    	return NeoJvmInstance.hasRelationshipType( name );    	
    }
     
	/**
     * @deprecated Not required now that relationship types are {@link
     * RelationshipType created dynamically}. Will be removed in next release.
	 */
	public void registerEnumRelationshipTypes( 
		Class<? extends RelationshipType> relationshipTypes )
	{
		NeoJvmInstance.addEnumRelationshipTypes( relationshipTypes );
	}

	/**
     * @deprecated Not required now that relationship types are {@link
     * RelationshipType created dynamically}. Will be removed in next release.
	 */
    public RelationshipType createAndRegisterRelationshipType( String name )
    {
    	return NeoJvmInstance.registerRelationshipType( name, true );
    }
    
	/**
     * @deprecated Not required now that relationship types are {@link
     * RelationshipType created dynamically}. Will be removed in next release.
	 */
    public RelationshipType registerRelationshipType( String name )
    {
    	return NeoJvmInstance.registerRelationshipType( name, false );
    }

	/**
     * @deprecated Not required now that relationship types are {@link
     * RelationshipType created dynamically}. Will be removed in next release.
	 */
    public void registerRelationshipTypes( Iterable<RelationshipType> types )
    {
    	for ( RelationshipType type : types )
    	{
    		NeoJvmInstance.registerRelationshipType( type.name(), true );
    	}
    }
    
	/**
     * @deprecated Not required now that relationship types are {@link
     * RelationshipType created dynamically}. Will be removed in next release.
	 */
    public void registerRelationshipTypes( RelationshipType[] types )
    {
       	for ( RelationshipType type : types )
    	{
    		NeoJvmInstance.registerRelationshipType( type.name(), true );
    	}
    }
}
