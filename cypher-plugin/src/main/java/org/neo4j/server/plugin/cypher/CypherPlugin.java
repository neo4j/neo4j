/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.plugin.cypher;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.CypherResultRepresentation;
import org.neo4j.server.rest.repr.Representation;

@Description( "DEPRECATED (go to /db/data/cypher): A server side plugin for the Cypher Query Language" )
public class CypherPlugin extends ServerPlugin
{

    @Name( "execute_query" )
    @Description( "execute a query" )
    @PluginTarget( GraphDatabaseService.class )
    public Representation executeScript(
            @Source final GraphDatabaseService neo4j,
            @Description( "The query string" ) @Parameter( name = "query", optional = false ) final String query,
            @Description( "The query parameters" ) @Parameter( name = "params", optional = true ) Map parameters,
            @Description( "The return format. Default is Neo4j REST. Allowed: 'json-data-table' "
                          + "to return Google Data Table JSON." ) @Parameter( name = "format", optional = true ) final String format )
            throws BadInputException
    {
        if ( parameters == null )
        {
            parameters = new HashMap<String, Object>();
        }
        ExecutionResult result;
        try
        {
            ExecutionEngine engine = new ExecutionEngine( neo4j );
            result = engine.execute( query, parameters );
            return new CypherResultRepresentation( result, false );
        }
        catch ( Exception e )
        {
            throw new BadInputException( e );
        }
    }

}
