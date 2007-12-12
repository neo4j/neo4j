/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
	private static final String VERSION = "NeoStore v0.9.3";
	 
	private static final int RECORD_SIZE = 0;
	 
	private NodeStore nodeStore;
	private PropertyStore propStore;
	private RelationshipStore relStore;
	private RelationshipTypeStore relTypeStore;

	public NeoStore( Map<?,?> config )
	{
		super( ( String ) config.get( "neo_store" ), config );
	}

	public NeoStore( String fileName )
	{
		super( fileName );
	}

	/**
	 * Initializes the node,relationship,property and relationship type stores.
	 */
	@Override
	protected void initStorage()
	{
		relTypeStore = new RelationshipTypeStore( 
			getStorageFileName() + ".relationshiptypestore.db", getConfig() );
		propStore = new PropertyStore( 
			getStorageFileName() + ".propertystore.db", getConfig() );
		relStore = new RelationshipStore( getStorageFileName()
			+ ".relationshipstore.db", getConfig() );
		nodeStore = new NodeStore( 
			getStorageFileName() + ".nodestore.db", getConfig() );
	}
	
	/**
	 * Closes the node,relationship,property and relationship type stores.
	 */
	@Override
	protected void closeStorage()
	{
		relTypeStore.close();
		relTypeStore = null;
		propStore.close();
		propStore = null;
		relStore.close();
		relStore = null;
		nodeStore.close();
		nodeStore = null;
	}
	
	public void flushAll()
	{
		relTypeStore.flushAll();
		propStore.flushAll();
		relStore.flushAll();
		nodeStore.flushAll();
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
	public void makeStoreOk()
	{
		relTypeStore.makeStoreOk();
		propStore.makeStoreOk();
		relStore.makeStoreOk();
		nodeStore.makeStoreOk();
		super.makeStoreOk();
	}
	
	public ReadFromBuffer getNewReadFromBuffer()
	{
		return null;
	}
	
	public void releaseReadFromBuffer( ReadFromBuffer buffer )
	{
		
	}
}
