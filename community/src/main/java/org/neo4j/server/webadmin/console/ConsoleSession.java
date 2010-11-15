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

import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.gremlin.GremlinScriptEngine;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class ConsoleSession
{
    protected ScriptEngine scriptEngine;
    protected StringWriter outputWriter;
    private Database database;

    public ConsoleSession( Database database )
    {
        this.database = database;
        scriptEngine = createGremlinScriptEngine();
    }

    /**
     * Take some gremlin script, evaluate it in the context of this gremlin
     * session, and return the result.
     *
     * @param script
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<String> evaluate( String script )
    {
        try
        {
            resetOutputWriter();

            List<Object> resultLines = (List<Object>)scriptEngine.eval( script );

            // Handle output data
            List<String> outputLines = new ArrayList<String>();

            // Handle eval() result
            String[] printLines = outputWriter.toString().split( "\n" );

            if ( printLines.length > 0 && printLines[ 0 ].length() > 0 )
            {
                for ( String printLine : printLines )
                {
                    outputLines.add( printLine );
                }
            }

            if ( resultLines == null
                    || resultLines.size() == 0
                    || ( resultLines.size() == 1 && ( resultLines.get( 0 ) == null || resultLines.get(
                    0 ).toString().length() == 0 ) ) )
            {
                // Result was empty, add empty text if there was also no IO
                // output
                if ( outputLines.size() == 0 )
                {
                    outputLines.add( "" );
                }
            } else
            {
                // Make sure all lines are strings
                for ( Object resultLine : resultLines )
                {
                    outputLines.add( resultLine.toString() );
                }
            }

            return outputLines;
        }
        catch ( ScriptException e )
        {
            return exceptionToResultList( e );
        }
        catch ( RuntimeException e )
        {
            e.printStackTrace();
            return exceptionToResultList( e );
        }
    }

    /**
     * Destroy the internal gremlin evaluator and replace it with a clean slate.
     */
    public synchronized void reset()
    {
        // #run() will pick up on this and create a new script engine. This
        // ensures it is instantiated in the correct thread context.
        this.scriptEngine = null;
    }


    //
    // INTERNALS
    //

    private List<String> exceptionToResultList( Exception e )
    {
        ArrayList<String> resultList = new ArrayList<String>();

        resultList.add( e.getMessage() );

        return resultList;
    }

    private void resetOutputWriter()
    {
        outputWriter = new StringWriter();
        scriptEngine.getContext().setWriter( outputWriter );
        scriptEngine.getContext().setErrorWriter( outputWriter );
    }

    //
    // GREMLIN CODE
    //

    public TransactionalGraph getGremlinWrappedGraph()
            throws DatabaseBlockedException
    {
        return new Neo4jGraph( database.graph, database.indexService );
    }

    public ScriptEngine createGremlinScriptEngine()
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


}
