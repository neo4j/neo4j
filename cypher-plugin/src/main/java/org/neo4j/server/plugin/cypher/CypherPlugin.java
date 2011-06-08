/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.neo4j.helpers.collection.IteratorUtil.asIterable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.CypherResultRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.neo4j.tbd.SyntaxError;
import org.neo4j.tbd.commands.Query;
import org.neo4j.tbd.javacompat.ExecutionEngine;
import org.neo4j.tbd.javacompat.Projection;
import org.neo4j.tbd.javacompat.SunshineParser;

/* This is a class that will represent a server side
 * Gremlin plugin and will return JSON
 * for the following use cases:
 * Add/delete vertices and edges from the graph.
 * Manipulate the graph indices.
 * Search for elements of a graph.
 * Load graph data from a file or URL.
 * Make use of JUNG algorithms.
 * Make use of SPARQL queries over OpenRDF-based graphs.
 * and much, much more.
 */

@Description( "A server side Query language plugin for the Neo4j REST server" )
public class CypherPlugin extends ServerPlugin
{

    private SunshineParser parser;
    private ExecutionEngine engine;

    @Name( "execute_query" )
    @Description( "execute a query with 'g' set to the Neo4jGraph and 'results' containing the results." )
    @PluginTarget( GraphDatabaseService.class )
    public Representation executeScript(
            @Source final GraphDatabaseService neo4j,
            @Description( "The query string" ) @Parameter( name = "query", optional = false ) final String query )
    {
        parser = new SunshineParser();
        Projection result;
        try
        {
            Query compiledQuery = parser.parse( query );
            engine = new ExecutionEngine( neo4j );
            result = engine.execute( compiledQuery );
            return new CypherResultRepresentation( result );
        }
        catch ( SyntaxError e )
        {
            e.printStackTrace();
        }
        return ValueRepresentation.emptyRepresentation();
    }

}
