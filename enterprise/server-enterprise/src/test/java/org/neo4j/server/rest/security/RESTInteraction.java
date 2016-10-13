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
package org.neo4j.server.rest.security;

import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.server.HTTP;

import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

class RESTInteraction extends AbstractRESTInteraction
{

    RESTInteraction( Map<String,String> config ) throws IOException
    {
        super( config );
    }

    @Override
    String commitPath()
    {
        return "db/data/transaction/commit";
    }

    @Override
    HTTP.RawPayload constructQuery( String query )
    {
        return quotedJson( "{'statements':[{'statement':'" +
                           query.replace( "'", "\\'" ).replace( "\"", "\\\"" )
                           + "'}]}" );
    }

    @Override
    void consume( Consumer<ResourceIterator<Map<String,Object>>> resultConsumer, JsonNode data )
    {
        if ( data.has( "results" ) && data.get( "results" ).has( 0 ) )
        {
            resultConsumer.accept( new RESTResult( data.get( "results" ).get( 0 ) ) );
        }
    }

    @Override
    protected HTTP.Response authenticate( String principalCredentials )
    {
        return HTTP.withHeaders( HttpHeaders.AUTHORIZATION, principalCredentials ).request( POST, commitURL() );
    }

    private class RESTResult extends AbstractRESTResult
    {
        RESTResult( JsonNode fullResult )
        {
            super( fullResult );
        }

        @Override
        protected JsonNode getRow( JsonNode data, int i )
        {
            return data.get( i ).get( "row" );
        }
    }
}
