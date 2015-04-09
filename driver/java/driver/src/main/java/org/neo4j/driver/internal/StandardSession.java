/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.spi.Connection;

public class StandardSession implements Session
{
    public static final Map<String,Value> NO_PARAMETERS = new HashMap<>();

    private final Connection connection;

    /** Called when a transaction object is closed */
    private final Runnable txCleanup = new Runnable()
    {
        @Override
        public void run()
        {
            currentTransaction = null;
        }
    };

    private Transaction currentTransaction;

    public StandardSession( Connection connection )
    {
        this.connection = connection;
    }

    @Override
    public Result run( String statement, Map<String,Value> parameters )
    {
        ensureNoOpenTransaction();
        ResultBuilder resultBuilder = new ResultBuilder();
        connection.run( statement, parameters, resultBuilder );
        connection.pullAll( resultBuilder );
        connection.sync();
        return resultBuilder.build();
    }

    @Override
    public Result run( String statement )
    {
        return run( statement, NO_PARAMETERS );
    }

    @Override
    public void close()
    {
        if ( currentTransaction != null )
        {
            try
            {
                currentTransaction.close();
            }
            catch ( Throwable e )
            {
                // Best-effort
            }
        }
        connection.close();
    }

    @Override
    public Transaction newTransaction()
    {
        ensureNoOpenTransaction();
        return currentTransaction = new StandardTransaction( connection, txCleanup );
    }

    private void ensureNoOpenTransaction()
    {
        if ( currentTransaction != null )
        {
            throw new ClientException( "Please close the currently open transaction object before running " +
                                       "more statements on the session level." );
        }
    }
}
