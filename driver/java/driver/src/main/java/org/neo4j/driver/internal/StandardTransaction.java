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

import java.util.Map;

import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.spi.Connection;

import static java.util.Collections.EMPTY_MAP;

public class StandardTransaction implements Transaction
{
    private final Connection conn;
    private final Runnable cleanup;

    private enum State
    {
        /** The transaction is running with no explicit success or failure marked */
        ACTIVE,

        /** Running, user marked for success, meaning it'll get committed */
        MARKED_SUCCESS,

        /** User marked as failed, meaning it'll be rolled back. */
        MARKED_FAILED,

        /**
         * An error has occurred, transaction can no longer be used and no more messages will be sent for this
         * transaction.
         */
        FAILED
    }

    private State state = State.ACTIVE;

    public StandardTransaction( Connection conn, Runnable cleanup )
    {
        this.conn = conn;
        this.cleanup = cleanup;

        // Note there is no sync here, so this will just get queued locally
        conn.run( "BEGIN", EMPTY_MAP, null );
        conn.discardAll();
    }

    @Override
    public void success()
    {
        if ( state == State.ACTIVE )
        {
            state = State.MARKED_SUCCESS;
        }
    }

    @Override
    public void failure()
    {
        if ( state == State.ACTIVE || state == State.MARKED_SUCCESS )
        {
            state = State.MARKED_FAILED;
        }
    }

    @Override
    public void close()
    {
        try
        {
            if ( state == State.MARKED_SUCCESS )
            {
                conn.run( "COMMIT", EMPTY_MAP, null );
                conn.discardAll();
                conn.sync();
            }
            else if ( state == State.MARKED_FAILED || state == State.ACTIVE )
            {
                // If none of the things we've put in the queue have been sent off, there is no need to
                // do this, we could just clear the queue. Future optimization.
                conn.run( "ROLLBACK", EMPTY_MAP, null );
                conn.discardAll();
            }
        }
        finally
        {
            cleanup.run();
        }
    }

    @Override
    @SuppressWarnings( "unchecked" )
    public Result run( String statement, Map<String,Value> parameters )
    {
        ensureNotFailed();

        try
        {
            ResultBuilder resultBuilder = new ResultBuilder();
            conn.run( statement, parameters, resultBuilder );
            conn.pullAll( resultBuilder );
            conn.sync();
            return resultBuilder.build();
        }
        catch ( Neo4jException e )
        {
            state = State.FAILED;
            throw e;
        }
    }

    @Override
    public Result run( String statement )
    {
        return run( statement, EMPTY_MAP );
    }

    private void ensureNotFailed()
    {
        if ( state == State.FAILED )
        {
            throw new ClientException(
                    "Cannot run more statements in this transaction, because previous statements in the " +
                    "transaction has failed and the transaction has been rolled back. Please start a new" +
                    " transaction to run another statement." );
        }
    }
}
