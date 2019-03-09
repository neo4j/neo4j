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
import java.util.Optional;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.v1.runtime.TransactionStateMachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.v4.runtime.TransactionStateMachineV4SPI;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

public class TransactionStateMachineSPIProviderV4 implements TransactionStateMachineSPIProvider
{
    private final Duration txAwaitDuration;
    private final Clock clock;
    private final BoltChannel boltChannel;
    private final String defaultDatabaseName;
    private final DatabaseManager databaseManager;

    public TransactionStateMachineSPIProviderV4( DatabaseManager databaseManager, String defaultDatabaseName, BoltChannel boltChannel, Duration txAwaitDuration,
            Clock clock )
    {
        this.databaseManager = databaseManager;
        this.defaultDatabaseName = defaultDatabaseName;
        this.boltChannel = boltChannel;
        this.txAwaitDuration = txAwaitDuration;
        this.clock = clock;
    }

    @Override
    public TransactionStateMachineSPI getTransactionStateMachineSPI( String databaseName, StatementProcessorReleaseManager resourceReleaseManger )
            throws BoltIOException
    {
        if ( Objects.equals( databaseName, ABSENT_DB_NAME ) )
        {
            databaseName = defaultDatabaseName;
        }
        Optional<DatabaseContext> databaseContextOptional = databaseManager.getDatabaseContext( databaseName );
        if ( !databaseContextOptional.isPresent() )
        {
            throw new BoltIOException( Status.Request.Invalid,
                    String.format( "The database requested does not exists. " + "Requested database name: '%s'.", databaseName ) );
        }
        DatabaseContext databaseContext = databaseContextOptional.get();
        return new TransactionStateMachineV4SPI( databaseContext, boltChannel, txAwaitDuration, clock, resourceReleaseManger );
    }
}
