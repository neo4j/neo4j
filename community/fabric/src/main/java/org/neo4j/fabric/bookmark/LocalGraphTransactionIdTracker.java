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
package org.neo4j.fabric.bookmark;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public class LocalGraphTransactionIdTracker
{
    private final TransactionIdTracker transactionIdTracker;
    private final DatabaseIdRepository databaseIdRepository;
    private final Duration bookmarkTimeout;

    public LocalGraphTransactionIdTracker( TransactionIdTracker transactionIdTracker, DatabaseIdRepository databaseIdRepository, Duration bookmarkTimeout )
    {
        this.transactionIdTracker = transactionIdTracker;
        this.databaseIdRepository = databaseIdRepository;
        this.bookmarkTimeout = bookmarkTimeout;
    }

    public void awaitSystemGraphUpToDate( long transactionId )
    {
        transactionIdTracker.awaitUpToDate( NAMED_SYSTEM_DATABASE_ID, transactionId, bookmarkTimeout );
    }

    /**
     * Unlike {@link #awaitSystemGraphUpToDate(long)}, the caller does not know which graph is System one
     * and this method will find it in the the submitted Graph UUID to transaction Id map.
     */
    public void awaitSystemGraphUpToDate( Map<UUID,Long> graphUuid2TxIdMapping )
    {
        Long systemDbTxId = graphUuid2TxIdMapping.get( NAMED_SYSTEM_DATABASE_ID.databaseId().uuid() );

        if ( systemDbTxId != null )
        {
            awaitSystemGraphUpToDate( systemDbTxId );
        }
    }

    public void awaitGraphUpToDate( Location.Local location, long transactionId )
    {
        var namedDatabaseId = getNamedDatabaseId( location );
        transactionIdTracker.awaitUpToDate( namedDatabaseId, transactionId, bookmarkTimeout );
    }

    public long getTransactionId( Location.Local location )
    {
        var namedDatabaseId = getNamedDatabaseId( location );
        return transactionIdTracker.newestTransactionId( namedDatabaseId );
    }

    private NamedDatabaseId getNamedDatabaseId( Location.Local location )
    {
        DatabaseId databaseId = DatabaseIdFactory.from( location.getUuid() );
        var namedDatabaseId = databaseIdRepository.getById( databaseId );
        if ( namedDatabaseId.isEmpty() )
        {
            // this can only happen when the database has just been deleted or someone tempered with a bookmark
            throw new IllegalArgumentException( "A local graph could not be mapped to a database" );
        }

        return namedDatabaseId.get();
    }
}
