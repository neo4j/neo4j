package org.neo4j.impl.nioneo.store;


import java.io.IOException;
import java.util.Map;

import org.neo4j.impl.nioneo.store.AbstractDynamicStore;

/**
 * Dynamic store that stores strings. 
 */
class DynamicStringStore extends AbstractDynamicStore
{
	// store version, each store ends with this string (byte encoded)
	private static final String VERSION = "StringPropertyStore v0.9";
	
	public DynamicStringStore( String fileName, Map config ) 
		throws IOException
	{
		super( fileName, config );
	}

	public DynamicStringStore( String fileName ) 
		throws IOException
	{
		super( fileName );
	}
	
	public String getTypeAndVersionDescriptor()
	{
		return VERSION;
	}
	
	public static void createStore( String fileName, 
		int blockSize ) throws IOException
	{
		createEmptyStore( fileName, blockSize, VERSION );
	}
	
	public String getString( int blockId ) throws IOException
	{
		return new String( get( blockId ) );
	}
}

