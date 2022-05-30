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
package org.neo4j.bolt.protocol.v40.transaction;

import java.time.Clock;
import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecutor;
import org.neo4j.bolt.protocol.common.message.result.BoltResult;
import org.neo4j.bolt.protocol.common.message.result.BoltResultHandle;
import org.neo4j.bolt.protocol.common.transaction.AbstractTransactionStateMachineSPI;
import org.neo4j.bolt.protocol.common.transaction.result.AdaptingBoltQuerySubscriber;
import org.neo4j.bolt.protocol.common.transaction.statement.StatementProcessorReleaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class TransactionStateMachineV4SPI extends AbstractTransactionStateMachineSPI {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(TransactionStateMachineV4SPI.class);

    protected final NamedDatabaseId namedDatabaseId;

    public TransactionStateMachineV4SPI(
            BoltGraphDatabaseServiceSPI boltGraphDatabaseServiceSPI,
            BoltChannel boltChannel,
            SystemNanoClock clock,
            StatementProcessorReleaseManager resourceReleaseManager,
            String transactionId) {
        super(boltGraphDatabaseServiceSPI, boltChannel, clock, resourceReleaseManager, transactionId);
        this.namedDatabaseId = boltGraphDatabaseServiceSPI.getNamedDatabaseId();
    }

    @Override
    protected BoltResultHandle newBoltResultHandle(
            String statement, MapValue params, BoltQueryExecutor boltQueryExecutor) {
        return new BoltResultHandleV4(statement, params, boltQueryExecutor);
    }

    @Override
    public boolean supportsNestedStatementsInTransaction() {
        return true;
    }

    private class BoltResultHandleV4 extends AbstractBoltResultHandle {

        BoltResultHandleV4(String statement, MapValue params, BoltQueryExecutor boltQueryExecutor) {
            super(statement, params, boltQueryExecutor);
        }

        @Override
        protected BoltResult newBoltResult(QueryExecution result, AdaptingBoltQuerySubscriber subscriber, Clock clock) {
            return new CypherAdapterStreamV4(result, subscriber, clock, namedDatabaseId.name());
        }
    }
}
