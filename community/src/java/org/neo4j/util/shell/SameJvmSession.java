/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.util.shell;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Session} optimized to use with a {@link SameJvmClient}.
 */
public class SameJvmSession implements Session
{
	private Map<String, Serializable> properties =
		new HashMap<String, Serializable>();
	
	public void set( String key, Serializable value )
	{
		this.properties.put( key, value );
	}
	
	public Serializable get( String key )
	{
		return this.properties.get( key );
	}

	public Serializable remove( String key )
	{
		return this.properties.remove( key );
	}

	public String[] keys()
	{
		return this.properties.keySet().toArray(
			new String[ this.properties.size() ] );
	}
}
