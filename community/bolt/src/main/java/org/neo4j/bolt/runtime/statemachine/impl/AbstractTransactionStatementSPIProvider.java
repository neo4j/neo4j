/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.runtime.statemachine.impl;

import static java.lang.String.format;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;

import java.util.Objects;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPIProvider;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.SystemNanoClock;

public abstract class AbstractTransactionStatementSPIProvider implements TransactionStateMachineSPIProvider {
    protected final SystemNanoClock clock;
    protected final BoltChannel boltChannel;
    protected final BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI;
    protected final MemoryTracker memoryTracker;

    public AbstractTransactionStatementSPIProvider(
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            BoltChannel boltChannel,
            SystemNanoClock clock,
            MemoryTracker memoryTracker) {
        this.boltGraphDatabaseManagementServiceSPI = boltGraphDatabaseManagementServiceSPI;
        this.clock = clock;
        this.boltChannel = boltChannel;
        this.memoryTracker = memoryTracker.getScopedMemoryTracker();
    }

    protected abstract TransactionStateMachineSPI newTransactionStateMachineSPI(
            BoltGraphDatabaseServiceSPI activeBoltGraphDatabaseServiceSPI,
            StatementProcessorReleaseManager resourceReleaseManager,
            String transactionId)
            throws BoltIOException;

    @Override
    public TransactionStateMachineSPI getTransactionStateMachineSPI(
            String databaseName, StatementProcessorReleaseManager resourceReleaseManager, String transactionId)
            throws BoltProtocolBreachFatality, BoltIOException {
        String selectedDatabaseName = selectDatabaseName(databaseName);

        try {
            var boltGraphDatabaseServiceSPI =
                    boltGraphDatabaseManagementServiceSPI.database(selectedDatabaseName, memoryTracker);
            return newTransactionStateMachineSPI(boltGraphDatabaseServiceSPI, resourceReleaseManager, transactionId);
        } catch (DatabaseNotFoundException e) {
            throw new BoltIOException(
                    Status.Database.DatabaseNotFound,
                    format("Database does not exist. Database name: '%s'.", selectedDatabaseName));
        } catch (UnavailableException e) {
            throw new BoltIOException(
                    Status.Database.DatabaseUnavailable, format("Database '%s' is unavailable.", selectedDatabaseName));
        }
    }

    protected String selectDatabaseName(String databaseName) throws BoltProtocolBreachFatality {
        // old versions of protocol does not support passing database name and any name that
        if (!Objects.equals(databaseName, ABSENT_DB_NAME)) {
            // This bolt version shall NOT provide us a db name.
            throw new BoltProtocolBreachFatality(format(
                    "Database selection by name not supported by Bolt protocol version lower than BoltV4. "
                            + "Please contact your Bolt client author to report this bug in the client code. Requested database name: '%s'.",
                    databaseName));
        }
        return boltChannel.defaultDatabase();
    }

    @Override
    public void releaseTransactionStateMachineSPI() {
        memoryTracker.reset();
    }
}
