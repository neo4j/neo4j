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
package org.neo4j.server.plugin.gremlin;

import java.util.Map;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleBindings;

import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.ObjectToRepresentationConverter;
import org.neo4j.server.rest.repr.Representation;

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
    private GraphDatabaseSetting<String> replacementDecision = new GraphDatabaseSetting.OptionsSetting("org.neo4j.server.plugin.gremlin.replacement", ScriptCountingEngineReplacementDecision.class.getName(), CountingEngineReplacementDecision.class.getName() )
    {
    };

    private final String g = "g";
    private volatile ScriptEngine engine;
    private EngineReplacementDecision engineReplacementDecision = null;

    private ScriptEngine createQueryEngine()
    {
        return new ScriptEngineManager().getEngineByName( "gremlin-groovy" );
    }

    @Name( "execute_script" )
    @Description( "execute a Gremlin script with 'g' set to the Neo4jGraph and 'results' containing the results. Only results of one object type is supported." )
    @PluginTarget( GraphDatabaseService.class )
    public Representation executeScript(
            @Source final GraphDatabaseService neo4j,
            @Description( "The Gremlin script" ) @Parameter( name = "script", optional = false ) final String script,
            @Description( "JSON Map of additional parameters for script variables" ) @Parameter( name = "params", optional = true ) final Map params ) throws BadInputException
    {

        // Initialize engine replacement decision if not done before
        if (engineReplacementDecision == null)
        {
            Config config = ((GraphDatabaseAPI) neo4j).getKernelData().getConfig();
            String decisionClass;
            if (config.isSet( replacementDecision ))
                decisionClass = config.<String>get(replacementDecision);
            else
                decisionClass = ScriptCountingEngineReplacementDecision.class.getName();

            try
            {
                engineReplacementDecision = (EngineReplacementDecision) getClass().getClassLoader().loadClass( decisionClass ).newInstance();
            }
            catch ( Throwable e1 )
            {
                engineReplacementDecision = new ScriptCountingEngineReplacementDecision(  );
            }
        }

        try
        {
            engineReplacementDecision.beforeExecution( script );

            final Bindings bindings = createBindings( neo4j, params );

            final Object result = engine().eval( script, bindings );
            return ObjectToRepresentationConverter.convert( result );
        }
        catch ( final Exception e )
        {
            throw new BadInputException( e.getMessage() );
        }
    }

    private Bindings createBindings( GraphDatabaseService neo4j, Map params )
    {
        final Bindings bindings = createInitialBinding( neo4j );
        if ( params != null )
        {
            bindings.putAll( params );
        }
        return bindings;
    }

    private Bindings createInitialBinding( GraphDatabaseService neo4j )
    {
        final Bindings bindings = new SimpleBindings();
        final Neo4jGraph graph = new Neo4jGraph( neo4j, false );
        bindings.put( g, graph );
        return bindings;
    }

    private ScriptEngine engine()
    {
        if ( this.engine == null
             || engineReplacementDecision.mustReplaceEngine() )
        {
            this.engine = createQueryEngine();
        }
        return this.engine;
    }

    public Representation getRepresentation( final Object data )
    {
        return ObjectToRepresentationConverter.convert( data );
    }

}
