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

import groovy.lang.Binding;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptException;

import org.codehaus.groovy.tools.shell.IO;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;

import com.tinkerpop.blueprints.pgm.TransactionalGraph;
import com.tinkerpop.blueprints.pgm.impls.neo4j.Neo4jGraph;
import com.tinkerpop.gremlin.console.ResultHookClosure;

public class GremlinSession implements ScriptSession
{
    protected GremlinWebConsole scriptEngine;
    protected StringWriter outputWriter;
    private Database database;
    private IO io;
    private ByteArrayOutputStream baos;

    public GremlinSession( Database database )
    {
        this.database = database;
        baos = new ByteArrayOutputStream();
        BufferedOutputStream out = new BufferedOutputStream( baos );
        io = new IO( System.in, out, out );
        Map bindings = new HashMap();
        bindings.put( "g", getGremlinWrappedGraph() );
        scriptEngine = new GremlinWebConsole(new Binding( bindings ), io );
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
        scriptEngine.groovy.execute( script );
        String result = baos.toString();
        resetIO();
        return result;

    }

    private void resetIO()
    {
        baos = new ByteArrayOutputStream();
        BufferedOutputStream out = new BufferedOutputStream( baos );
        IO io = new IO( System.in, out, out );
        scriptEngine.groovy.setResultHook( new ResultHookClosure( scriptEngine.groovy, io ) );
    }

    private Object runScript( String script ) throws ScriptException
    {
        resetOutputWriter();
        Transaction tx = database.graph.beginTx();
        Object resultLines = evaluate( script );
        tx.success();
        tx.finish();
        return resultLines;
    }

    private void resetOutputWriter()
    {
        outputWriter = new StringWriter();

    }

    private TransactionalGraph getGremlinWrappedGraph()
            throws DatabaseBlockedException
    {
        return new Neo4jGraph( database.graph );
    }
}
