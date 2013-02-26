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
package org.neo4j.shell.kernel.apps.cypher;

import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Map;

@Service.Implementation(App.class)
public class Dump extends Start
{
    private static final int BATCH_SIZE = 20000;

    {
        addOptionDefinition( "s", new OptionDefinition( OptionValueType.NONE,
                "Simple export for smaller graphs with no parameters" ) );
    }

    @Override
    public String getDescription()
    {
        return "Executes a Cypher query to export a subgraph. Usage: DUMP start <rest of query>;\n" +
                "Example: DUMP start n = node({self}) MATCH n-[r]->m RETURN n,r,m;\n" +
                "where {self} will be replaced with the current location in the graph." +
                "Please, note that the query must end with a semicolon. Other parameters are\n" +
                "taken from shell variables, see 'help export'.";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        final boolean simpleExport = isSimpleExport( parser );
        if ( parser.arguments().isEmpty() )
        {
            final SubGraph graph = DatabaseSubGraph.from(getServer().getDb());
            export( graph, out, simpleExport );
            return Continuation.INPUT_COMPLETE;
        }

        AppCommandParser newParser = newParser( parser, simpleExport );
        return super.exec( newParser, session, out );
    }

    private AppCommandParser newParser( AppCommandParser parser, boolean simpleExport ) throws ShellException
    {
        String newLine = simpleExport ? removeOption( parser.getLineWithoutApp(), "s" ) : parser.getLineWithoutApp();
        AppCommandParser newParser = newParser( newLine );
        newParser.options().putAll( parser.options() );
        return newParser;
    }

    private AppCommandParser newParser( final String line ) throws ShellException
    {
        try
        {
            return new AppCommandParser( getServer(), line );
        } catch ( Exception e )
        {
            throw new ShellException( "Error parsing input " + line );
        }
    }

    private String removeOption( final String lineWithoutApp, final String option )
    {
        return lineWithoutApp.replaceFirst( "-[" + option + "]\\s*", "" );
    }

    private boolean isSimpleExport( AppCommandParser parser )
    {
        return parser.options().containsKey( "s" );
    }

    private void export( SubGraph subGraph, Output out, boolean simple ) throws RemoteException, ShellException
    {
        if ( simple )
        {
            final SubGraphExporter exporter = new SubGraphExporter( subGraph );
            exporter.export( out );
        }
        else
        {
            final ShellSubGraphExporter exporter = new ShellSubGraphExporter( subGraph, BATCH_SIZE );
            exporter.export( out );
        }
    }

    @Override
    protected void handleResult( Output out, ExecutionResult result, long startTime, Session session, AppCommandParser parser ) throws RemoteException, ShellException
    {
        final SubGraph subGraph = CypherResultSubGraph.from(result, false);
        export( subGraph, out, isSimpleExport( parser ) );
    }

    @Override
    protected void setVariables( Session session, Collection<Map<String, Object>> rows )
    {
    }
}
