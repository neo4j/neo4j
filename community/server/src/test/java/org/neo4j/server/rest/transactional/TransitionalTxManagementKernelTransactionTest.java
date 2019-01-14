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
package org.neo4j.server.rest.transactional;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransitionalTxManagementKernelTransactionTest
{

    private GraphDatabaseFacade databaseFacade = mock( GraphDatabaseFacade.class );
    private ThreadToStatementContextBridge contextBridge = mock( ThreadToStatementContextBridge.class );
    private LoginContext loginContext = AnonymousContext.read();
    private KernelTransaction.Type type = KernelTransaction.Type.implicit;

    @Test
    public void reopenStartTransactionWithCustomTimeoutIfSpecified()
    {
        TransitionalTxManagementKernelTransaction managementKernelTransaction =
                new TransitionalTxManagementKernelTransaction( databaseFacade, type, loginContext, 10, contextBridge );

        managementKernelTransaction.reopenAfterPeriodicCommit();

        verify( databaseFacade, times( 2 ) ).beginTransaction( type, loginContext, 10, TimeUnit.MILLISECONDS);
    }

    @Test
    public void reopenStartDefaultTransactionIfTimeoutNotSpecified()
    {
        TransitionalTxManagementKernelTransaction managementKernelTransaction =
                new TransitionalTxManagementKernelTransaction( databaseFacade, type, loginContext, -1, contextBridge );

        managementKernelTransaction.reopenAfterPeriodicCommit();

        verify( databaseFacade, times( 2 ) ).beginTransaction( type, loginContext );
    }
}
