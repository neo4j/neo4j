/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.http.cypher;

import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class TransitionalTxManagementKernelTransaction
{
    private final GraphDatabaseAPI db;
    private final KernelTransaction.Type type;
    private final LoginContext loginContext;
    private final long customTransactionTimeout;
    private final ClientConnectionInfo connectionInfo;

    private InternalTransaction tx;

    TransitionalTxManagementKernelTransaction( GraphDatabaseAPI db, KernelTransaction.Type type, LoginContext loginContext,
            ClientConnectionInfo connectionInfo, long customTransactionTimeout )
    {
        this.db = db;
        this.type = type;
        this.loginContext = loginContext;
        this.customTransactionTimeout = customTransactionTimeout;
        this.connectionInfo = connectionInfo;
        this.tx = startTransaction();
    }

    public InternalTransaction getInternalTransaction()
    {
        return tx;
    }

    public void terminate()
    {
        tx.terminate();
    }

    public void rollback()
    {
        tx.rollback();
    }

    public void commit()
    {
        tx.commit();
    }

    void closeTransactionForPeriodicCommit()
    {
        tx.close();
    }

    void reopenAfterPeriodicCommit()
    {
        tx = startTransaction();
    }

    private InternalTransaction startTransaction()
    {
        return customTransactionTimeout > GraphDatabaseSettings.UNSPECIFIED_TIMEOUT ? db.beginTransaction( type, loginContext, connectionInfo,
                customTransactionTimeout, TimeUnit.MILLISECONDS ) : db.beginTransaction( type, loginContext, connectionInfo );
    }
}
