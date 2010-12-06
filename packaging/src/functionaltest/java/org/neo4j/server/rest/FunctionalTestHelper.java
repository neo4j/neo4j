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

import java.io.IOException;

import org.neo4j.server.NeoServer;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;

public final class FunctionalTestHelper
{
    private NeoServer server;
    private GraphDbHelper helper;

    public FunctionalTestHelper(NeoServer server) {
        if(server.getDatabase() == null) {
            throw new RuntimeException("Server must be started before using " + getClass().getName());
        }
        this.helper = new GraphDbHelper(server.getDatabase());
        this.server = server;
    }
 
    public GraphDbHelper getGraphDbHelper() {
        return helper;
    }

    void assertLegalJson( String entity ) throws IOException
    {
        JsonHelper.jsonToMap( entity );
    }

    String dataUri()
    {
        return server.restApiUri().toString();
    }

    String managementUri()
    {
        return server.managementApiUri().toString();
    }

    String nodeUri()
    {
        return dataUri() + "node";
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
        return dataUri() + "relationship";
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
        return dataUri() + "index";
    }

	String mangementUri()
	{
		return server.managementApiUri().toString();
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
