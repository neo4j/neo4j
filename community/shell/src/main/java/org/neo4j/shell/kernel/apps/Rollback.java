/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.Variables;

@Service.Implementation(App.class)
public class Rollback extends NonTransactionProvidingApp
{
    @Override
    public String getDescription()
    {
        return "Rolls back all open transactions";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        if ( parser.getLineWithoutApp().trim().length() > 0 )
        {
            out.println( "Error: ROLLBACK should  be run without trailing arguments" );
            return Continuation.INPUT_COMPLETE;
        }

        Transaction tx = Begin.currentTransaction( getServer() );
        if ( tx == null )
        {
            throw Commit.fail( session, "Not in a transaction" );
        } else
        {
            try
            {
                session.remove( Variables.TX_COUNT );
                tx.rollback();
                out.println( "Transaction rolled back" );
                return Continuation.INPUT_COMPLETE;
            } catch ( SystemException e )
            {
                throw new ShellException( e.getMessage() );
            }
        }
    }
}
