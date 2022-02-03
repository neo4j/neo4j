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
package org.neo4j.bolt.v44.runtime;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltQueryExecutor;
import org.neo4j.bolt.runtime.BoltResult;
import org.neo4j.bolt.runtime.BoltResultHandle;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.impl.BoltAdapterSubscriber;
import org.neo4j.bolt.v4.runtime.TransactionStateMachineV4SPI;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.values.virtual.MapValue;

public class TransactionStateMachineV44SPI extends TransactionStateMachineV4SPI
{
    public TransactionStateMachineV44SPI( BoltGraphDatabaseServiceSPI boltGraphDatabaseServiceSPI, BoltChannel boltChannel,
                                          SystemNanoClock clock,
                                          StatementProcessorReleaseManager resourceReleaseManager,
                                          String transactionId )
    {
        super( boltGraphDatabaseServiceSPI, boltChannel, clock, resourceReleaseManager, transactionId );
    }

    @Override
    protected BoltResultHandle newBoltResultHandle( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor )
    {
        return new BoltResultHandleV44( statement, params, boltQueryExecutor );
    }

    private class BoltResultHandleV44 extends AbstractBoltResultHandle
    {

        BoltResultHandleV44( String statement, MapValue params, BoltQueryExecutor boltQueryExecutor )
        {
            super( statement, params, boltQueryExecutor );
        }

        @Override
        protected BoltResult newBoltResult( QueryExecution result, BoltAdapterSubscriber subscriber, Clock clock )
        {
            return new CypherAdapterStreamV44( result, subscriber, clock, databaseReference.alias().name() );
        }
    }
}
