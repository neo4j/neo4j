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
package org.neo4j.bolt.protocol.common.transaction.statement;

import static java.lang.String.format;
import static org.neo4j.bolt.protocol.v40.messaging.util.MessageMetadataParserV40.ABSENT_DB_NAME;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.protocol.common.connector.tx.TransactionOwner;
import org.neo4j.bolt.protocol.common.transaction.TransactionStateMachineSPI;
import org.neo4j.bolt.protocol.common.transaction.TransactionStateMachineSPIProvider;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.SystemNanoClock;

public abstract class AbstractTransactionStatementSPIProvider implements TransactionStateMachineSPIProvider {
    protected final SystemNanoClock clock;
    protected final TransactionOwner owner;
    protected final BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI;
    protected final MemoryTracker memoryTracker;

    public AbstractTransactionStatementSPIProvider(
            BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            TransactionOwner owner,
            SystemNanoClock clock) {
        this.boltGraphDatabaseManagementServiceSPI = boltGraphDatabaseManagementServiceSPI;
        this.clock = clock;
        this.owner = owner;
        this.memoryTracker = owner.memoryTracker().getScopedMemoryTracker();
    }

    protected abstract TransactionStateMachineSPI newTransactionStateMachineSPI(
            BoltGraphDatabaseServiceSPI activeBoltGraphDatabaseServiceSPI,
            StatementProcessorReleaseManager resourceReleaseManager,
            String transactionId)
            throws BoltIOException;

    @Override
    public TransactionStateMachineSPI getTransactionStateMachineSPI(
            String databaseName, StatementProcessorReleaseManager resourceReleaseManager, String transactionId)
            throws BoltIOException {
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

    protected String selectDatabaseName(String databaseName) {
        if (ABSENT_DB_NAME.equals(databaseName)) {
            return this.owner.selectedDefaultDatabase();
        }

        return databaseName;
    }

    @Override
    public void releaseTransactionStateMachineSPI() {
        memoryTracker.reset();
    }
}
