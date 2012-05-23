/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell;

import java.io.Serializable;
import java.util.Map;

/**
 * A session (or environment) for a shell client.
 */
public interface Session extends Serializable
{
	/**
	 * Sets a session value.
	 * @param key the session key.
	 * @param value the value.
	 */
	void set( String key, Serializable value );
	
	/**
	 * @param key the key to get the session value for.
	 * @return the value for the {@code key} or {@code null} if not found.
	 */
	Serializable get( String key );
	
	/**
	 * Removes a value from the session.
	 * @param key the session key to remove.
	 * @return the removed value, or {@code null} if none.
	 */
	Serializable remove( String key );
	
	/**
	 * @return all the available session keys.
	 */
	String[] keys();
	
	/**
	 * Returns the session as a {@link Map} representation. Changes in the
	 * returned instance won't be reflected in the session.
	 * @return the session as a {@link Map}.
	 */
	Map<String, Serializable> asMap();
}
