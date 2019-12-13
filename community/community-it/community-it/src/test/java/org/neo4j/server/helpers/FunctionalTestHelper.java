/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server.helpers;

import java.net.URI;

import org.neo4j.server.rest.domain.GraphDbHelper;

public final class FunctionalTestHelper
{
    private final TestWebContainer server;
    private final GraphDbHelper helper;

    public FunctionalTestHelper( TestWebContainer container )
    {
        this.helper = new GraphDbHelper( container.getDefaultDatabase() );
        this.server = container;
    }

    public GraphDbHelper getGraphDbHelper()
    {
        return helper;
    }

    private String databaseUri()
    {
        return databaseUri( "neo4j" );
    }

    private String databaseUri( String databaseName )
    {
        return String.format( "%sdb/%s/", server.getBaseUri(), databaseName );
    }

    public URI baseUri()
    {
        return server.getBaseUri();
    }

    public String txCommitUri()
    {
        return databaseUri() + "tx/commit";
    }

    public String txCommitUri( String databaseName )
    {
        return databaseUri( databaseName ) + "tx/commit";
    }
}
