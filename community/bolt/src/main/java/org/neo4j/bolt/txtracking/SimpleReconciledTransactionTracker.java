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
package org.neo4j.bolt.txtracking;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * A {@link ReconciledTransactionTracker} used for community databases that do not have a reconciler.
 */
public class SimpleReconciledTransactionTracker implements ReconciledTransactionTracker
{
    private final DatabaseManagementService dbService;
    private final Log log;

    public SimpleReconciledTransactionTracker( DatabaseManagementService dbService, LogService logService )
    {
        this.dbService = dbService;
        this.log = logService.getInternalLog( getClass() );
    }

    @Override
    public void initialize( long reconciledTransactionId )
    {
        throw new UnsupportedOperationException( "Initialization is not supported" );
    }

    @Override
    public long getLastReconciledTransactionId()
    {
        try
        {
            var systemDb = (GraphDatabaseAPI) dbService.database( SYSTEM_DATABASE_NAME );
            if ( systemDb.isAvailable( 0 ) )
            {
                var txIdStore = systemDb.getDependencyResolver().resolveDependency( TransactionIdStore.class );
                return txIdStore.getLastClosedTransactionId();
            }
        }
        catch ( Exception e )
        {
            log.warn( "Unable to get last reconciled transaction ID", e );
        }
        return NO_RECONCILED_TRANSACTION_ID;
    }

    @Override
    public void setLastReconciledTransactionId( long reconciledTransactionId )
    {
        throw new UnsupportedOperationException( "Updates are not supported" );
    }
}
