/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.rest;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerStartupException;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import java.io.IOException;

public abstract class FunctionalTestBase
{
    protected static NeoServer server;
    protected static GraphDbHelper helper;

    @BeforeClass
    public static void startNeoServer() throws ServerStartupException
    {
        server = ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
        helper = new GraphDbHelper( server.database() );
    }

    @AfterClass
    public static void stopNeoServer() throws Exception
    {
        ServerTestUtils.nukeServer();
    }

    void assertLegalJson( String entity ) throws IOException
    {
        JsonHelper.jsonToMap( entity );
    }

    String baseUri()
    {
        return NeoServer.getServer_FOR_TESTS_ONLY_KITTENS_DIE_WHEN_YOU_USE_THIS().restApiUri().toString();
    }

    String nodeUri()
    {
        return baseUri() + "node";
    }

    String nodeUri( long id )
    {
        return nodeUri() + "/" + id;
    }

    String nodePropertiesUri( long id )
    {
        return nodeUri( id ) + "/properties";
    }

    String nodePropertyUri( long id, String key )
    {
        return nodePropertiesUri( id ) + "/" + key;
    }

    String relationshipUri()
    {
        return baseUri() + "relationship";
    }

    String relationshipUri( long id )
    {
        return relationshipUri() + "/" + id;
    }

    String relationshipPropertiesUri( long id )
    {
        return relationshipUri( id ) + "/properties";
    }

    String relationshipPropertyUri( long id, String key )
    {
        return relationshipPropertiesUri( id ) + "/" + key;
    }

    String relationshipsUri( long nodeId, String dir, String... types )
    {
        StringBuilder typesString = new StringBuilder();
        for ( String type : types )
        {
            typesString.append( typesString.length() > 0 ? "&" : "" );
            typesString.append( type );
        }
        return nodeUri( nodeId ) + "/relationships/" + dir + "/" + typesString;
    }

    String indexUri()
    {
        return baseUri() + "index";
    }

    String indexUri( String indexName )
    {
        return indexUri() + "/" + indexName;
    }

    String indexUri( String indexName, String key, Object value )
    {
        return indexUri( indexName ) + "/" + key + "/" + value;
    }
}
