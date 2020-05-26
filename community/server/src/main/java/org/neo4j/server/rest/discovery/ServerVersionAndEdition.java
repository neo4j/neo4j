/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.rest.discovery;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.neo4j.kernel.internal.Version;
import org.neo4j.server.NeoWebServer;

public class ServerVersionAndEdition
{
    private final Map<String, String> serverInfo;

    ServerVersionAndEdition( NeoWebServer neoWebServer )
    {
        this( neoDatabaseVersion(), neoServerEdition( neoWebServer ) );
    }

    public ServerVersionAndEdition( String version, String edition )
    {
        serverInfo = new HashMap<>();
        serverInfo.put( "neo4j_version", version );
        serverInfo.put( "neo4j_edition", edition );
    }

    public void forEach( BiConsumer<String,String> consumer )
    {
        serverInfo.forEach( consumer );
    }

    private static String neoDatabaseVersion()
    {
        return Version.getKernel().getReleaseVersion();
    }

    private static String neoServerEdition( NeoWebServer neoWebServer )
    {
        return neoWebServer.getDbmsInfo().edition.toString();
    }
}
