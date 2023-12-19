/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.stresstests;

import java.util.concurrent.TimeoutException;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;

class TxHelp
{
    static boolean isTransient( Throwable e )
    {
        return e != null && (
                        e instanceof TimeoutException ||
                        e instanceof DatabaseShutdownException ||
                        e instanceof TransactionFailureException ||
                        e instanceof AcquireLockTimeoutException ||
                        e instanceof TransientTransactionFailureException ||
                        isInterrupted( e.getCause() ) );
    }

    static boolean isInterrupted( Throwable e )
    {
        if ( e == null )
        {
            return false;
        }

        if ( e instanceof InterruptedException )
        {
            Thread.interrupted();
            return true;
        }

        return isInterrupted( e.getCause() );
    }
}
