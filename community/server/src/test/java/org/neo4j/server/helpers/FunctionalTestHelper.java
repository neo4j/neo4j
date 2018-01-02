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
package org.neo4j.server.helpers;

import com.sun.jersey.api.client.Client;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.NeoServer;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.web.RestfulGraphDatabase;

import static org.neo4j.server.rest.web.RestfulGraphDatabase.PATH_AUTO_INDEX;

public final class FunctionalTestHelper
{
    private final NeoServer server;
    private final GraphDbHelper helper;

    public static final Client CLIENT = Client.create();
    private RestRequest request;

    public FunctionalTestHelper( NeoServer server )
    {
        if ( server.getDatabase() == null )
        {
            throw new RuntimeException( "Server must be started before using " + getClass().getName() );
        }
        this.helper = new GraphDbHelper( server.getDatabase() );
        this.server = server;
        this.request = new RestRequest(server.baseUri().resolve("db/data/"));
    }

    public static Matcher<String[]> arrayContains( final String element )
    {
        return new TypeSafeMatcher<String[]>()
        {
            private String[] array;

            @Override
            public void describeTo( Description descr )
            {
                descr.appendText( "The array " )
                        .appendText( Arrays.toString( array ) )
                        .appendText( " does not contain <" )
                        .appendText( element )
                        .appendText( ">" );
            }

            @Override
            public boolean matchesSafely( String[] array )
            {
                this.array = array;
                for ( String string : array )
                {
                    if ( element == null )
                    {
                        if ( string == null ) return true;
                    }
                    else if ( element.equals( string ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public GraphDbHelper getGraphDbHelper()
    {
        return helper;
    }

    public String dataUri()
    {
        return server.baseUri().toString() + "db/data/";
    }

    public String nodeUri()
    {
        return dataUri() + "node";
    }

    public String nodeUri( long id )
    {
        return nodeUri() + "/" + id;
    }

    public String nodePropertiesUri( long id )
    {
        return nodeUri( id ) + "/properties";
    }

    public String nodePropertyUri( long id, String key )
    {
        return nodePropertiesUri( id ) + "/" + key;
    }

    String relationshipUri()
    {
        return dataUri() + "relationship";
    }

    public String relationshipUri( long id )
    {
        return relationshipUri() + "/" + id;
    }

    public String relationshipPropertiesUri( long id )
    {
        return relationshipUri( id ) + "/properties";
    }

    String relationshipPropertyUri( long id, String key )
    {
        return relationshipPropertiesUri( id ) + "/" + key;
    }

    public String relationshipsUri( long nodeId, String dir, String... types )
    {
        StringBuilder typesString = new StringBuilder();
        for ( String type : types )
        {
            typesString.append( typesString.length() > 0 ? "&" : "" );
            typesString.append( type );
        }
        return nodeUri( nodeId ) + "/relationships/" + dir + "/" + typesString;
    }

    public String indexUri()
    {
        return dataUri() + "index/";
    }

    String nodeAutoIndexUri()
    {
        return indexUri() + "auto/node/";
    }

    String relationshipAutoIndexUri()
    {
        return indexUri() + "auto/relationship/";
    }

    public String nodeIndexUri()
    {
        return indexUri() + "node/";
    }

    public String relationshipIndexUri()
    {
        return indexUri() + "relationship/";
    }

    public String managementUri()
    {
        return server.baseUri()
                .toString() + "db/manage";
    }

    public String indexNodeUri( String indexName )
    {
        return nodeIndexUri() + indexName;
    }

    public String indexNodeUri( String indexName, String key, Object value )
    {
        return indexNodeUri( indexName ) + "/" + key + "/" + value;
    }


    public String indexRelationshipUri( String indexName )
    {
        return relationshipIndexUri() + indexName;
    }

    public String indexRelationshipUri( String indexName, String key, Object value )
    {
        return indexRelationshipUri( indexName ) + "/" + key + "/" + value;
    }

    public String extensionUri()
    {
        return dataUri() + "ext";
    }

    String extensionUri( String name )
    {
        return extensionUri() + "/" + name;
    }

    String graphdbExtensionUri( String name, String method )
    {
        return extensionUri( name ) + "/graphdb/" + method;
    }

    String nodeExtensionUri( String name, String method, long id )
    {
        return extensionUri( name ) + "/node/" + id + "/" + method;
    }

    String relationshipExtensionUri( String name, String method, long id )
    {
        return extensionUri( name ) + "/relationship/" + id + "/" + method;
    }

    public GraphDatabaseAPI getDatabase()
    {
        return server.getDatabase().getGraph();
    }

    public String webAdminUri()
    {
        // the trailing slash prevents a 302 redirect
        return server.baseUri()
                .toString() + "webadmin" + "/";
    }

    public JaxRsResponse get(String path) {
        return request.get(path);
    }

    public JaxRsResponse get(String path, String data) {
        return request.get(path, data);
    }

    public JaxRsResponse delete(String path) {
        return request.delete(path);
    }

    public JaxRsResponse post(String path, String data) {
        return request.post(path, data);
    }

    public void put(String path, String data) {
        request.put(path, data);
    }

    public long getNodeIdFromUri( String nodeUri )
    {
        return Long.valueOf( nodeUri.substring( nodeUri.lastIndexOf( "/" ) +1 , nodeUri.length() ) );
    }

    public long getRelationshipIdFromUri( String relationshipUri )
    {
        return getNodeIdFromUri( relationshipUri );
    }

    public Map<String, Object> removeAnyAutoIndex( Map<String, Object> map )
    {
        Map<String, Object> result = new HashMap<String, Object>();
        for ( Map.Entry<String, Object> entry : map.entrySet() )
        {
            Map<?, ?> innerMap = (Map<?,?>) entry.getValue();
            String template = innerMap.get( "template" ).toString();
            if ( !template.contains( PATH_AUTO_INDEX.replace("{type}", RestfulGraphDatabase.NODE_AUTO_INDEX_TYPE) ) &&
                 !template.contains( PATH_AUTO_INDEX.replace("{type}", RestfulGraphDatabase.RELATIONSHIP_AUTO_INDEX_TYPE) ) &&
                 !template.contains( "_auto_" ) )
                result.put( entry.getKey(), entry.getValue() );
        }
        return result;
    }

    public URI baseUri()
    {
        return server.baseUri();
    }

    public String browserUri()
    {
        return server.baseUri().toString() + "browser/";
    }
}
