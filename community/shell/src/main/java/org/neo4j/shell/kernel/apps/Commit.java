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

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

@Service.Implementation(App.class)
public class Commit extends ReadOnlyGraphDatabaseApp
{
    public static final String TX_COUNT = "TX_COUNT";

    @Override
    public String getDescription()
    {
        return "Commits a transaction";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        if ( parser.getLineWithoutApp().trim().length() > 0 )
        {
            out.println( "Error: COMMIT should  be run without trailing arguments" );
            return Continuation.INPUT_COMPLETE;
        }

        Integer txCount = (Integer) session.get( TX_COUNT );

        if ( txCount == null || txCount.equals( 0 ) )
        {
            throw new ShellException( "Not in a transaction" );
        } else if ( txCount.equals( 1 ) )
        {
            Transaction tx;
            try
            {
                tx = ((AbstractGraphDatabase) getServer().getDb()).getTxManager().getTransaction();
                if ( tx == null )
                {
                    throw fail( session, "Not in a transaction" );
                }
            } catch ( SystemException e )
            {
                throw fail( session, "Not in a transaction" );
            }

            try
            {
                tx.commit();
                session.remove( TX_COUNT );
                out.println( "Transaction committed" );
                return Continuation.INPUT_COMPLETE;
            } catch ( Exception e )
            {
                throw fail( session, e.getMessage() );
            }
        } else
        {
            session.set( TX_COUNT, --txCount );
            out.println( String.format( "Nested transaction committed (Tx count: %d)", txCount ) );
            return Continuation.INPUT_COMPLETE;
        }
    }

    public static ShellException fail( Session session, String message ) throws ShellException
    {
        session.remove( TX_COUNT );
        return new ShellException( message );
    }
}
