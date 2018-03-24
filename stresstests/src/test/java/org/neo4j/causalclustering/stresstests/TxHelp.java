/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
