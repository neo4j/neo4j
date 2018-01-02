/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.web;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.GraphDbHelper;

public class WebHelper
{
    private final URI baseUri;
    private final GraphDbHelper helper;

    public WebHelper( URI baseUri, Database database )
    {
        this.baseUri = baseUri;
        this.helper = new GraphDbHelper( database );

    }

    public URI createNode()
    {
        long nodeId = helper.createNode();
        try
        {
            return new URI( baseUri.toString() + "/" + nodeId );
        }
        catch ( URISyntaxException e )
        {
            throw new RuntimeException( e );
        }
    }

    public URI createNodeWithProperties( Map<String, Object> props )
    {
        URI nodeUri = createNode();
        setNodeProperties( nodeUri, props );
        return nodeUri;
    }

    private void setNodeProperties( URI nodeUri, Map<String, Object> props )
    {
        helper.setNodeProperties( extractNodeId( nodeUri ), props );
    }

    private long extractNodeId( URI nodeUri )
    {
        String path = nodeUri.getPath();
        if ( path.startsWith( "/" ) )
        {
            path = path.substring( 1 );
        }

        return Long.parseLong( path );
    }
}
