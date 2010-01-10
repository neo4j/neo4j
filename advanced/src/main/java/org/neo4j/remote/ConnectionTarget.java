/*
 * Copyright (c) 2008-2009 "Neo Technology,"
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
package org.neo4j.remote;

/**
 * Represents a server that a {@link RemoteGraphDatabase graph database client}
 * can connect to.
 * 
 * @author Tobias Ivarsson
 */
public interface ConnectionTarget
{
    /**
     * Connect to the remote site.
     * @return The connection to the remote site.
     */
    RemoteConnection connect();

    /**
     * Connect to the remote site.
     * @param username
     *            The name of the user that makes the connection.
     * @param password
     *            The password for the user that makes the connection.
     * @return The connection to the remote site.
     */
    RemoteConnection connect( String username, String password );
}
