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
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

@Service.Implementation(App.class)
public class Begin extends ReadOnlyGraphDatabaseApp
{
    @Override
    public String getDescription()
    {
        return "Opens a transaction";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session, Output out )
            throws ShellException, RemoteException
    {
        Transaction tx = currentTransaction( getServer() );

        getServer().getDb().beginTx();
        Integer txCount = (Integer) session.get( "tx_count" );

        int count;
        if ( txCount == null )
        {
            if ( tx == null )
            {
                count = 0;
            } else
            {
                count = 1;
            }
        } else
        {
            count = txCount;
        }

        session.set( "tx_count", ++count );
        out.println("Transaction started");
        return null;
    }

    public static Transaction currentTransaction( GraphDatabaseShellServer server ) throws ShellException
    {
        try
        {
            return ((AbstractGraphDatabase) server.getDb()).getTxManager().getTransaction();
        } catch ( SystemException e )
        {
            throw new ShellException( e.getMessage() );
        }
    }
}
