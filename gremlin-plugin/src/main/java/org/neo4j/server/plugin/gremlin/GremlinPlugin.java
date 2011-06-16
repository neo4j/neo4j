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
package org.neo4j.server.plugin.gremlin;

import com.tinkerpop.blueprints.pgm.Edge;
import com.tinkerpop.blueprints.pgm.Graph;
import com.tinkerpop.blueprints.pgm.Vertex;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jEdge;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jVertex;
import com.tinkerpop.gremlin.pipes.util.Table;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.server.rest.repr.GremlinTableRepresentation;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ValueRepresentation;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.List;

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

@Description( "A server side Gremlin plugin for the Neo4j REST server" )
public class GremlinPlugin extends ServerPlugin
{

    private final String g = "g";
    private static final ScriptEngine engine = new ScriptEngineManager().getEngineByName( "gremlin" );

    @Name( "execute_script" )
    @Description( "execute a Gremlin script with 'g' set to the Neo4jGraph and 'results' containing the results. Only results of one object type is supported." )
    @PluginTarget( GraphDatabaseService.class )
    public Representation executeScript(
            @Source final GraphDatabaseService neo4j,
            @Description( "The Gremlin script" ) @Parameter( name = "script", optional = false ) final String script )
    {
        final Neo4jGraph graph = new Neo4jGraph( neo4j );
        final Bindings bindings = new SimpleBindings();
        bindings.put( g, graph );

        try
        {
            final Object result = this.engine.eval( script, bindings );
            return getRepresentation( graph, result );
        }
        catch ( final ScriptException e )
        {
            return ValueRepresentation.string( e.getMessage() );
        }
    }

    public static Representation getRepresentation( final Neo4jGraph graph,
            final Object result )
    {
        if ( result instanceof Iterable )
        {
            RepresentationType type = RepresentationType.STRING;
            final List<Representation> results = new ArrayList<Representation>();
            if ( result instanceof Table )
            {
                type = RepresentationType.STRING;
                results.add( new GremlinTableRepresentation( (Table) result, graph ) );
                return new ListRepresentation( type, results );
            }
            for ( final Object r : (Iterable) result )
            {
                if ( r instanceof Vertex )
                {
                    type = RepresentationType.NODE;
                    results.add( new NodeRepresentation(
                            ( (Neo4jVertex) r ).getRawVertex() ) );
                }
                else if ( r instanceof Edge )
                {
                    type = RepresentationType.RELATIONSHIP;
                    results.add( new RelationshipRepresentation(
                            ( (Neo4jEdge) r ).getRawEdge() ) );
                }
                else if ( r instanceof Graph )
                {
                    type = RepresentationType.STRING;
                    results.add( ValueRepresentation.string( graph.getRawGraph().toString() ) );
                }
                else if ( r instanceof Double || r instanceof Float )
                {
                    type = RepresentationType.DOUBLE;
                    results.add( ValueRepresentation.number( ( (Number) r ).doubleValue() ) );
                }
                else if ( r instanceof Long || r instanceof Integer )
                {
                    type = RepresentationType.LONG;
                    results.add( ValueRepresentation.number( ( (Number) r ).longValue() ) );
                }
                else
                {
                    System.out.println("GremlinPlugin: got back" + r);
                    type = RepresentationType.STRING;
                    results.add( ValueRepresentation.string( r.toString() ) );
                }
            }
            return new ListRepresentation( type, results );
        }
        else
        {
            if ( result instanceof Vertex )
            {
                return new NodeRepresentation(
                        ( (Neo4jVertex) result ).getRawVertex() );
            }
            else if ( result instanceof Edge )
            {
                return new RelationshipRepresentation(
                        ( (Neo4jEdge) result ).getRawEdge() );
            }
            else if ( result instanceof Graph )
            {
                return ValueRepresentation.string( graph.getRawGraph().toString() );
            }
            else if ( result instanceof Double || result instanceof Float )
            {
                return ValueRepresentation.number( ( (Number) result ).doubleValue() );
            }
            else if ( result instanceof Long || result instanceof Integer )
            {
                return ValueRepresentation.number( ( (Number) result ).longValue() );
            }
            else
            {
                return ValueRepresentation.string( result + "" );
            }
        }
    }

}
