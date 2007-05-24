package org.neo4j.impl.nioneo.store;


import java.io.IOException;
import java.util.Map;

/**
 * This class contains the references to the "NodeStore,RelationshipStore,
 * PropertyStore and RelationshipTypeStore". NeoStore doesn't actually "store"
 * anything but extends the AbstractStore for the "type and version" 
 * validation performed in there.
 */
public class NeoStore extends AbstractStore
{
	// neo store version, store should end with this string
	// (byte encoded)
	private static final String VERSION = "NeoStore v0.9";
	 
	private static final int RECORD_SIZE = 0;
	 
	private NodeStore nodeStore;
	private PropertyStore propStore;
	private RelationshipStore relStore;
	private RelationshipTypeStore relTypeStore;

	public NeoStore( Map config ) throws IOException
	{
		super( ( String ) config.get( "neo_store" ), config );
	}

	public NeoStore( String fileName ) throws IOException
	{
		super( fileName );
	}

	/**
	 * Initializes the node,relationship,property and relationship type stores.
	 */
	@Override
	protected void initStorage() throws IOException
	{
		relTypeStore = new RelationshipTypeStore( 
			getStorageFileName() + ".relationshiptypestore.db", getConfig() );
		propStore = new PropertyStore( 
			getStorageFileName() + ".propertystore.db", getConfig() );
		relStore = new RelationshipStore( getStorageFileName()
			+ ".relationshipstore.db", getConfig() );
		nodeStore = new NodeStore( 
			getStorageFileName() + ".nodestore.db", getConfig() );
		nodeStore.setRelationshipStore( relStore );
		nodeStore.setPropertyStore( propStore );
		relStore.setPropertyStore( propStore );
	}
	
	/**
	 * Closes the node,relationship,property and relationship type stores.
	 */
	@Override
	protected void closeStorage() throws IOException
	{
		relTypeStore.close();
		propStore.close();
		relStore.close();
		nodeStore.close();
	}
	
	/**
	 * Passes a flush to the node,relationship,property and relationship type
	 * store.
	 */
	@Override
	public void flush( int txIdentifier ) throws IOException
	{
		relTypeStore.flush( txIdentifier );
		propStore.flush( txIdentifier );
		relStore.flush( txIdentifier );
		nodeStore.flush( txIdentifier );
	}
	
	/**
	 * Passes a forget to the node,relationship,property and relationship type
	 * store.
	 */
	@Override
	public void forget( int txIdentifier )
	{
		relTypeStore.forget( txIdentifier );
		propStore.forget( txIdentifier );
		relStore.forget( txIdentifier );
		nodeStore.forget( txIdentifier );
	}

	public String getTypeAndVersionDescriptor()
	{
		return VERSION;
	}
	
	public int getRecordSize()
	{
		return RECORD_SIZE;
	}
	
	/**
	 * Creates the neo,node,relationship,property and relationship type stores.
	 * 
	 * @param fileName The name of neo store
	 * @throws IOException If unable to create stores or name null
	 */
	public static void createStore( String fileName ) 
		throws IOException
	{
		createEmptyStore( fileName, VERSION );
		NodeStore.createStore( fileName + ".nodestore.db" );
		RelationshipStore.createStore( 
			fileName + ".relationshipstore.db" );
		PropertyStore.createStore( fileName + ".propertystore.db" );
		RelationshipTypeStore.createStore( fileName + 
			".relationshiptypestore.db" );
	}
	
	/**
	 * Returns the node store for this neo store.
	 * 
	 * @return The node store
	 */
	public NodeStore getNodeStore()
	{
		return nodeStore;
	}
	
	/**
	 * The relationship store for this neo store
	 *  
	 * @return The relationship store
	 */
	public RelationshipStore getRelationshipStore()
	{
		return relStore;
	}
	
	/**
	 * Returns the relationship type store for this neo store
	 * 
	 * @return The relationship type store
	 */
	public RelationshipTypeStore getRelationshipTypeStore()
	{
		return relTypeStore;
	}
	
	/**
	 * Returns the property store for this neo store.
	 * 
	 * @return The property store
	 */
	public PropertyStore getPropertyStore()
	{
		return propStore;
	}
	
	@Override
	protected void rebuildIdGenerator()
	{ // do nothing
	}
	
	@Override
	public void makeStoreOk() throws IOException
	{
		relTypeStore.makeStoreOk();
		propStore.makeStoreOk();
		relStore.makeStoreOk();
		nodeStore.makeStoreOk();
	}
	
	// validation not needed on this store
	@Override
	public void validate() {}
}
