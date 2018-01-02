/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.rmi.RemoteException;

import org.neo4j.cypher.export.CypherResultSubGraph;
import org.neo4j.cypher.export.DatabaseSubGraph;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

import static org.neo4j.helpers.Exceptions.launderedException;

@Service.Implementation( App.class )
public class Dump extends Start
{
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
        if ( parser.arguments().isEmpty() )
        {
            final SubGraph graph = DatabaseSubGraph.from(getServer().getDb());
            export( graph, out);
            return Continuation.INPUT_COMPLETE;
        }

        AppCommandParser newParser = newParser( parser );
        return super.exec( newParser, session, out );
    }

    private AppCommandParser newParser(AppCommandParser parser) throws ShellException
    {
        String newLine = parser.getLineWithoutApp();
        AppCommandParser newParser = newParser( newLine );
        newParser.options().putAll( parser.options() );
        return newParser;
    }

    private AppCommandParser newParser( final String line ) throws ShellException
    {
        try
        {
            return new AppCommandParser( getServer(), line );
        }
        catch ( Exception e )
        {
            throw launderedException( ShellException.class, "Error parsing input " + line, e );
        }
    }

    private void export(SubGraph subGraph, Output out) throws RemoteException, ShellException
    {
        new Exporter( subGraph ).export( out );
    }

    @Override
    protected void handleResult( Output out, Result result, long startTime ) throws RemoteException, ShellException
    {
        final SubGraph subGraph = CypherResultSubGraph.from(result, getServer().getDb(), false);
        export( subGraph, out);
    }
}
