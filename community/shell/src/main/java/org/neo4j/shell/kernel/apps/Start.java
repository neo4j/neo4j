/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.shell.kernel.apps;

import java.rmi.RemoteException;

import org.neo4j.cypher.SyntaxException;
import org.neo4j.cypher.commands.Query;
import org.neo4j.cypher.javacompat.CypherParser;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. renames a property. It
 * could also (regarding POSIX) move nodes, but it doesn't).
 */
@Service.Implementation( App.class )
public class Start extends GraphDatabaseApp
{
    public Start()
    {
        super();
    }

    @Override
    public String getDescription()
    {
        return "Executes a Cypher query. " +
        	"Usage: start <rest of query>";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
        throws ShellException, RemoteException
    {
        String query = "start";
        for ( String argument : parser.arguments() )
        {
            query += " " + argument;
        }
        CypherParser qparser = new CypherParser();
        ExecutionEngine engine = new ExecutionEngine( getServer().getDb() );
        try
        {
            Query cquery = qparser.parse( query );
            ExecutionResult result = engine.execute( cquery );
            out.println( result.toString() );
        }
        catch ( SyntaxException e )
        {
            throw ShellException.wrapCause( e );
        }
        return null;
    }
}
