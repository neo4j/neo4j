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
package org.neo4j.bolt.runtime;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.v1.runtime.StatementProcessorReleaseManager;
import org.neo4j.bolt.v4.runtime.TransactionStateMachineV4SPI;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_ID;

public class TransactionStateMachineSPIProviderV4 implements TransactionStateMachineSPIProvider
{
    private final Duration txAwaitDuration;
    private final Clock clock;
    private final BoltChannel boltChannel;
    private final DatabaseId defaultDatabaseId;
    private final DatabaseManager<?> databaseManager;

    public TransactionStateMachineSPIProviderV4( DatabaseManager<?> databaseManager, DatabaseId defaultDatabaseId, BoltChannel boltChannel,
            Duration txAwaitDuration, Clock clock )
    {
        this.databaseManager = databaseManager;
        this.defaultDatabaseId = defaultDatabaseId;
        this.boltChannel = boltChannel;
        this.txAwaitDuration = txAwaitDuration;
        this.clock = clock;
    }

    @Override
    public TransactionStateMachineSPI getTransactionStateMachineSPI( DatabaseId providedDatabaseId, StatementProcessorReleaseManager resourceReleaseManger )
            throws BoltIOException
    {
        var databaseId = Objects.equals( providedDatabaseId, ABSENT_DB_ID ) ? defaultDatabaseId : providedDatabaseId;

        DatabaseContext databaseContext = databaseManager.getDatabaseContext( databaseId )
                .orElseThrow( () -> new BoltIOException( Status.Database.DatabaseNotFound,
                    format( "The database requested does not exist. Requested database name: '%s'.", databaseId.name() ) ) );

        return new TransactionStateMachineV4SPI( databaseContext, boltChannel, txAwaitDuration, clock, resourceReleaseManger, databaseId );
    }
}
