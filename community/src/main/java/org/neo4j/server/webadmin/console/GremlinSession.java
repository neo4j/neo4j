/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.StringWriter;
import java.util.List;

public class GremlinSession implements ScriptSession
{
    protected ScriptEngine scriptEngine;
    protected StringWriter outputWriter;
    private Database database;

    public GremlinSession( Database database )
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
    @Override
    public String evaluate( String script )
    {
        try
        {
            List<Object> resultLines = runScript( script );

            StringBuilder result = new StringBuilder();
            result.append( outputWriter.toString() );

            if ( resultLines.size() > 0 )
            {
                for ( Object resultLine : resultLines )
                {
                    result.append( resultLine.toString() );
                }
            }

            return result.toString();
        } catch ( ScriptException e )
        {
            return e.getMessage();
        } catch ( RuntimeException e )
        {
            e.printStackTrace();
            return e.getMessage();
        }
    }

    private List<Object> runScript( String script )
            throws ScriptException
    {
        resetOutputWriter();
        Transaction tx = database.graph.beginTx();
        List<Object> resultLines = (List<Object>)scriptEngine.eval( script );
        tx.success();
        tx.finish();
        return resultLines;
    }

    private void resetOutputWriter()
    {
        outputWriter = new StringWriter();
        scriptEngine.getContext().setWriter( outputWriter );
        scriptEngine.getContext().setErrorWriter( outputWriter );
    }

    private TransactionalGraph getGremlinWrappedGraph()
            throws DatabaseBlockedException
    {
        return new Neo4jGraph( database.graph );
    }

    private ScriptEngine createGremlinScriptEngine()
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
            } catch ( Exception e )
            {
                // Om-nom-nom
            }

            return engine;
        } catch ( Throwable e )
        {
            // Pokemon catch b/c fails here get hidden until the server exits.
            e.printStackTrace();
            return null;
        }
    }

}
