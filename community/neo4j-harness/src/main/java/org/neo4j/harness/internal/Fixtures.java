/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.harness.internal;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.codehaus.jackson.JsonNode;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;

/**
 * Manages user-defined cypher fixtures that can be exercised against the server.
 */
public class Fixtures
{
    private static final String TX_COMMIT_ENDPOINT = "/db/data/transaction/commit";

    private final List<String> fixtureStatements = new LinkedList<>();
    private final String cypherSuffix = "cyp";

    private final FileFilter cypherFileOrDirectoryFilter = new FileFilter()
    {
        @Override
        public boolean accept( File file )
        {
            if(file.isDirectory())
            {
                return true;
            }
            String[] split = file.getName().split( "\\." );
            String suffix = split[split.length-1];
            return suffix.equals( cypherSuffix );
        }
    };

    public void add( File fixturePath )
    {
        try
        {
            if(fixturePath.isDirectory())
            {
                for ( File file : fixturePath.listFiles( cypherFileOrDirectoryFilter ) )
                {
                    add(file);
                }
                return;
            }
            add( FileUtils.readTextFile( fixturePath, Charset.forName( "UTF-8" ) ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Unable to read fixture file '"+fixturePath.getAbsolutePath()+"': " + e.getMessage(), e );
        }
    }

    public void add( String statement )
    {
        if(statement.trim().length() > 0)
        {
            fixtureStatements.add( statement );
        }
    }

    public void applyTo( URI serverBaseURI )
    {
        if(fixtureStatements.size() > 0)
        {
            Client client = new Client();
            ClientRequest req = ClientRequest.create()
                    .accept( MediaType.APPLICATION_JSON )
                    .entity( createJsonFrom( map( "statements", statementPayload() ) ),
                            MediaType.APPLICATION_JSON_TYPE )
                    .build( serverBaseURI.resolve( TX_COMMIT_ENDPOINT ), "POST" );
            ClientResponse response = client.handle( req );
            ensureInstallSuccessful( response );
        }
    }

    private void ensureInstallSuccessful( ClientResponse response )
    {
        String entity = response.getEntity( String.class );
        try
        {
            JsonNode errors = JsonHelper.jsonNode( entity ).get("errors");
            if(errors.size() > 0)
            {
                throw new RuntimeException( "Failed to install fixtures: " + errors.get(0).get("message").asText() );
            }
        }
        catch ( JsonParseException e )
        {
            throw new RuntimeException( "Fatal, server returned an invalid response, '"+e.getMessage()+"': " + entity );
        }
    }

    private List<Map<String, Object>> statementPayload()
    {
        List<Map<String,Object>> statements = new LinkedList<>();
        for ( String fixtureStatement : fixtureStatements )
        {
            statements.add(map("statement", fixtureStatement));
        }
        return statements;
    }
}
