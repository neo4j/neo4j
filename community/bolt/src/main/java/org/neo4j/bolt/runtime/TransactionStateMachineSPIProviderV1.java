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

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.v1.runtime.StatementProcessorReleaseManager;
import org.neo4j.bolt.v1.runtime.TransactionStateMachineV1SPI;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.time.SystemNanoClock;

public class TransactionStateMachineSPIProviderV1 extends DefaultDatabaseTransactionStatementSPIProvider
{
    TransactionStateMachineSPIProviderV1( DatabaseManagementService managementService, String activeDatabaseName, BoltChannel boltChannel,
            Duration awaitDuration, SystemNanoClock clock )
    {
        super( managementService, activeDatabaseName, boltChannel, awaitDuration, clock );
    }

    @Override
    protected TransactionStateMachineSPI newTransactionStateMachineSPI( GraphDatabaseFacade databaseFacade,
            StatementProcessorReleaseManager resourceReleaseManger )
    {
        return new TransactionStateMachineV1SPI( databaseFacade, boltChannel, txAwaitDuration, clock, resourceReleaseManger );
    }
}
