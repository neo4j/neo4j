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

package org.neo4j.server.webadmin.console;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.rest.domain.DatabaseBlockedException;
import org.neo4j.rest.domain.DatabaseLocator;
import org.neo4j.server.webadmin.domain.MockIndexService;

import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.gremlin.GremlinScriptEngine;

/**
 * Builds gremlin evaluators that come pre-packaged with astonishing connective
 * powers. Such powers include, but are not limited to, connecting to the REST
 * neo4j instance running under the hood of the webadmin system.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
@SuppressWarnings( "restriction" )
public class GremlinFactory
{

    protected volatile static boolean initiated = false;

    public static TransactionalGraph getGremlinWrappedGraph()
            throws DatabaseBlockedException
    {
        GraphDatabaseService dbInstance = DatabaseLocator.getGraphDatabase();
        System.out.println("GremlinFactory: " + dbInstance);
        TransactionalGraph graph;
        try
        {
            graph = new Neo4jGraph( dbInstance,
                    DatabaseLocator.getIndexService() );

        }
        catch ( UnsupportedOperationException e )
        {
            // Tempary until indexing is implemented in webadmin for remote
            // databases
            graph = new Neo4jGraph( dbInstance, new MockIndexService() );
        }

        return graph;
    }

    public static ScriptEngine createGremlinScriptEngine()
    {
        try
        {
            ScriptEngine engine = new GremlinScriptEngine();

            // Inject the local database
            TransactionalGraph graph = getGremlinWrappedGraph();

            engine.getBindings( ScriptContext.ENGINE_SCOPE ).put( "$_g", graph );

            try
            {
                engine.getBindings( ScriptContext.ENGINE_SCOPE ).put( "$_",
                        graph.getVertex( 0l ) );
            }
            catch ( Exception e )
            {
                // Om-nom-nom
            }

            return engine;
        }
        catch ( Throwable e )
        {
            // Pokemon catch b/c fails here get hidden until the server exits.
            e.printStackTrace();
            return null;
        }
    }

    protected synchronized void ensureInitiated()
    {
        if ( initiated == false )
        {
            new ConsoleGarbageCollector();
        }
    }
}
