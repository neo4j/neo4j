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
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseId;

import static java.lang.String.format;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_ID;

public abstract class DefaultDatabaseTransactionStatementSPIProvider implements TransactionStateMachineSPIProvider
{
    final Duration txAwaitDuration;
    final Clock clock;
    final BoltChannel boltChannel;
    private final DatabaseId defaultDatabaseId;
    private final DatabaseManager<?> databaseManager;

    public DefaultDatabaseTransactionStatementSPIProvider( DatabaseManager<?> databaseManager, DatabaseId defaultDatabaseId, BoltChannel boltChannel,
            Duration awaitDuration, Clock clock )
    {
        this.databaseManager = databaseManager;
        this.defaultDatabaseId = defaultDatabaseId;
        this.txAwaitDuration = awaitDuration;
        this.clock = clock;
        this.boltChannel = boltChannel;
    }

    @Override
    public TransactionStateMachineSPI getTransactionStateMachineSPI( DatabaseId databaseId, StatementProcessorReleaseManager resourceReleaseManger )
            throws BoltProtocolBreachFatality, BoltIOException
    {
        if ( !Objects.equals( databaseId, ABSENT_DB_ID ) )
        {
            // This bolt version shall NOT provide us a db name.
            throw new BoltProtocolBreachFatality( format( "Database selection by name not supported by Bolt protocol version lower than BoltV4. " +
                    "Please contact your Bolt client author to report this bug in the client code. Requested database name: '%s'.", databaseId.name() ) );
        }
        return newTransactionStateMachineSPI( getDefaultDatabase(), resourceReleaseManger );
    }

    protected abstract TransactionStateMachineSPI newTransactionStateMachineSPI( DatabaseContext activeDatabase,
            StatementProcessorReleaseManager resourceReleaseManger );

    private DatabaseContext getDefaultDatabase() throws BoltIOException
    {
        return databaseManager.getDatabaseContext( defaultDatabaseId )
                .orElseThrow( () -> new BoltIOException( Status.Database.DatabaseNotFound,
                        format( "Default database does not exists. Default database name: '%s'", defaultDatabaseId.name() ) ) );
    }
}
