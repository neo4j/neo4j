/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

@Service.Implementation( App.class )
public class Begin extends NonTransactionProvidingApp
{
    private static final String TRANSACTION = "TRANSACTION";

    @Override
    public String getDescription()
    {
        return "Opens a transaction";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws RemoteException
    {
        String lineWithoutApp = parser.getLineWithoutApp();
        if ( !acceptableText( lineWithoutApp ) )
        {
            out.println( "Error: To open a transaction, write BEGIN TRANSACTION" );
            return Continuation.INPUT_COMPLETE;
        }

        KernelTransaction tx = currentTransaction( getServer() );

        // This is a "begin" app so it will leave a transaction open. Don't close it in here
        getServer().getDb().beginTx();
        Integer txCount = session.getCommitCount();

        int count;
        if ( txCount == null )
        {
            if ( tx == null )
            {
                count = 0;
                out.println( "Transaction started" );
            }
            else
            {
                count = 1;
                out.println( "Warning: transaction found that was not started by the shell." );
            }
        }
        else
        {
            count = txCount;
            out.println( String.format( "Nested transaction started (Tx count: %d)", count + 1 ) );
        }

        session.setCommitCount( ++count );
        return Continuation.INPUT_COMPLETE;
    }

    private boolean acceptableText( String line )
    {
        return line != null && line.length() <= TRANSACTION.length() && TRANSACTION.startsWith( line.toUpperCase() );

    }

    public static KernelTransaction currentTransaction( GraphDatabaseShellServer server )
    {
        return server.getDb().getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class )
                .getTopLevelTransactionBoundToThisThread( false );
    }
}
