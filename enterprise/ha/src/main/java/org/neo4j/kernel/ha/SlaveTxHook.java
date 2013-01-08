/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import javax.transaction.Transaction;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.TxHook;

public class SlaveTxHook implements TxHook
{
    private final Broker broker;
    private final SlaveDatabaseOperations databaseOperations;
    private GraphDatabaseAPI spi;

    public SlaveTxHook( Broker broker, SlaveDatabaseOperations databaseOperations, GraphDatabaseAPI spi )
    {
        this.broker = broker;
        this.databaseOperations = databaseOperations;
        this.spi = spi;
    }

    @Override
    public void initializeTransaction( int eventIdentifier )
    {
        try
        {
            databaseOperations.receive( broker.getMaster().first().initializeTx( databaseOperations.getSlaveContext( eventIdentifier ) ) );
        }
        catch ( RuntimeException e )
        {
            databaseOperations.exceptionHappened( e );
            throw e;
        }
    }

    public boolean hasAnyLocks( Transaction tx )
    {
        return spi.getLockReleaser().hasLocks( tx );
    }

    public void finishTransaction( int eventIdentifier, boolean success )
    {
        try
        {
            databaseOperations.receive( broker.getMaster().first().finishTransaction(
                    databaseOperations.getSlaveContext( eventIdentifier ), success ) );
        }
        catch ( RuntimeException e )
        {
            databaseOperations.exceptionHappened( e );
            throw e;
        }
    }

    @Override
    public boolean freeIdsDuringRollback()
    {
        return false;
    }
}
