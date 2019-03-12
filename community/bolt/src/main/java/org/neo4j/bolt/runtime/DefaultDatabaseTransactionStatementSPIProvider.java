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
import org.neo4j.bolt.v1.runtime.StatementProcessorReleaseManager;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

public abstract class DefaultDatabaseTransactionStatementSPIProvider implements TransactionStateMachineSPIProvider
{
    final Duration txAwaitDuration;
    final Clock clock;
    final BoltChannel boltChannel;
    private final String defaultDatabaseName;
    private final DatabaseManager databaseManager;

    public DefaultDatabaseTransactionStatementSPIProvider( DatabaseManager databaseManager, String defaultDatabaseName, BoltChannel boltChannel,
            Duration awaitDuration, Clock clock )
    {
        this.databaseManager = databaseManager;
        this.defaultDatabaseName = defaultDatabaseName;
        this.txAwaitDuration = awaitDuration;
        this.clock = clock;
        this.boltChannel = boltChannel;
    }

    @Override
    public TransactionStateMachineSPI getTransactionStateMachineSPI( String databaseName, StatementProcessorReleaseManager resourceReleaseManger )
            throws BoltProtocolBreachFatality, BoltIOException
    {
        if ( !Objects.equals( databaseName, ABSENT_DB_NAME ) )
        {
            // This bolt version shall NOT provide us a db name.
            throw new BoltProtocolBreachFatality( format( "Database selection by name not supported by Bolt protocol version lower than BoltV4. " +
                    "Please contact your Bolt client author to report this bug in the client code. Requested database name: '%s'.", databaseName ) );
        }
        return newTransactionStateMachineSPI( getDefaultDatabase(), resourceReleaseManger );
    }

    protected abstract TransactionStateMachineSPI newTransactionStateMachineSPI( DatabaseContext activeDatabase,
            StatementProcessorReleaseManager resourceReleaseManger );

    private DatabaseContext getDefaultDatabase() throws BoltIOException
    {
        Optional<DatabaseContext> databaseContext = databaseManager.getDatabaseContext( defaultDatabaseName );
        if ( !databaseContext.isPresent() )
        {
            throw new BoltIOException( Status.Request.Invalid,
                    format( "Default database does not exists. Default database name: '%s'", defaultDatabaseName ) );
        }
        return databaseContext.get();
    }
}
