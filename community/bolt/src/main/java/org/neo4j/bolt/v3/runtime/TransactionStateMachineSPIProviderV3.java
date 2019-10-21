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
package org.neo4j.bolt.v3.runtime;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.impl.AbstractTransactionStatementSPIProvider;
import org.neo4j.time.SystemNanoClock;

public class TransactionStateMachineSPIProviderV3 extends AbstractTransactionStatementSPIProvider
{
    public TransactionStateMachineSPIProviderV3( BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementServiceSPI,
            String defaultDatabaseName, BoltChannel boltChannel,
            SystemNanoClock clock )
    {
        super( boltGraphDatabaseManagementServiceSPI, defaultDatabaseName, boltChannel, clock );
    }

    @Override
    protected TransactionStateMachineSPI newTransactionStateMachineSPI( BoltGraphDatabaseServiceSPI activeBoltGraphDatabaseServiceSPI,
            StatementProcessorReleaseManager resourceReleaseManger )
    {
        return new TransactionStateMachineV3SPI( activeBoltGraphDatabaseServiceSPI, boltChannel, clock, resourceReleaseManger );
    }
}
