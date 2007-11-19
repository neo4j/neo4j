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

import org.neo4j.impl.nioneo.store.AbstractDynamicStore;

/**
 * Dynamic store that stores strings. 
 */
class DynamicStringStore extends AbstractDynamicStore
{
	// store version, each store ends with this string (byte encoded)
	private static final String VERSION = "StringPropertyStore v0.9.3";
	
	public DynamicStringStore( String fileName, Map<?,?> config ) 
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
}

