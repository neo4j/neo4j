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

import java.time.Duration;
import java.util.Objects;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.v1.runtime.StatementProcessorReleaseManager;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static java.lang.String.format;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

public abstract class DefaultDatabaseTransactionStatementSPIProvider implements TransactionStateMachineSPIProvider
{
    final Duration txAwaitDuration;
    final SystemNanoClock clock;
    final BoltChannel boltChannel;
    private final String defaultDatabaseName;
    private final DatabaseManagementService managementService;

    public DefaultDatabaseTransactionStatementSPIProvider( DatabaseManagementService managementService, String defaultDatabaseName, BoltChannel boltChannel,
            Duration awaitDuration, SystemNanoClock clock )
    {
        this.managementService = managementService;
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

    protected abstract TransactionStateMachineSPI newTransactionStateMachineSPI( GraphDatabaseFacade activeDatabase,
            StatementProcessorReleaseManager resourceReleaseManger );

    private GraphDatabaseFacade getDefaultDatabase() throws BoltIOException
    {
        try
        {
            return (GraphDatabaseFacade) managementService.database( defaultDatabaseName );
        }
        catch ( DatabaseNotFoundException e )
        {
            throw new BoltIOException( Status.Database.DatabaseNotFound,
                    format( "Default database does not exists. Default database name: '%s'", defaultDatabaseName ) );
        }
    }
}
