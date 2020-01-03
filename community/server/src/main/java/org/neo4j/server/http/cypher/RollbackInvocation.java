/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.http.cypher;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.server.http.cypher.format.api.TransactionNotificationState;

/**
 * An invocation that produces output event stream representing a response to rollback request.
 */
class RollbackInvocation
{
    private final Log log;
    private final TransactionHandle transactionHandle;

    RollbackInvocation( Log log, TransactionHandle transactionHandle )
    {
        this.log = log;
        this.transactionHandle = transactionHandle;
    }

    void execute( OutputEventStream outputEventStream )
    {
        TransactionNotificationState transactionNotificationState = TransactionNotificationState.NO_TRANSACTION;
        try
        {
            if ( transactionHandle != null )
            {
                transactionHandle.ensureActiveTransaction();
                transactionHandle.rollback();

                transactionNotificationState = TransactionNotificationState.ROLLED_BACK;
            }
        }
        catch ( Exception e )
        {
            log.error( "Failed to roll back transaction.", e );
            outputEventStream.writeFailure( Status.Transaction.TransactionRollbackFailed, e.getMessage() );
            transactionNotificationState = TransactionNotificationState.UNKNOWN;
        }

        outputEventStream.writeTransactionInfo( transactionNotificationState, null, -1);
    }
}
