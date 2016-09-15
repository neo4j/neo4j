/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.security.enterprise.auth.plugin.api;

/**
 * The predefined keys of the auth token <tt>Map&lt;String,Object&gt;</tt>.
 */
public interface AuthToken
{
    String PRINCIPAL = "principal";
    String CREDENTIALS = "credentials";
    String REALM = "realm";

    /**
     * The corresponding value of this key is a <tt>Map<String,Object></tt> of custom parameters
     * as provided by the client. This can be used as a vehicle to connect a client application
     * with a server-side auth plugin.
     * Neo4j will act as a pure transport and will not inspect the contents of this map.
     */
    String PARAMETERS = "parameters";
}
