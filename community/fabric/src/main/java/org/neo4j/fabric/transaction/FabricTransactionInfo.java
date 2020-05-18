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
package org.neo4j.fabric.transaction;

import java.time.Duration;
import java.util.Map;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;

public class FabricTransactionInfo
{
    private final AccessMode accessMode;
    private final LoginContext loginContext;
    private final ClientConnectionInfo clientConnectionInfo;
    private final String databaseName;
    private final boolean implicitTransaction;
    private final Duration txTimeout;
    private final RoutingContext routingContext;
    private Map<String,Object> txMetadata;

    public FabricTransactionInfo( AccessMode accessMode,
            LoginContext loginContext,
            ClientConnectionInfo clientConnectionInfo,
            String databaseName,
            boolean implicitTransaction,
            Duration txTimeout,
            Map<String,Object> txMetadata,
            RoutingContext routingContext )
    {
        this.accessMode = accessMode;
        this.loginContext = loginContext;
        this.clientConnectionInfo = clientConnectionInfo;
        this.databaseName = databaseName;
        this.implicitTransaction = implicitTransaction;
        this.txTimeout = txTimeout;
        this.txMetadata = txMetadata;
        this.routingContext = routingContext;
    }

    public AccessMode getAccessMode()
    {
        return accessMode;
    }

    public LoginContext getLoginContext()
    {
        return loginContext;
    }

    public ClientConnectionInfo getClientConnectionInfo()
    {
        return clientConnectionInfo;
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public boolean isImplicitTransaction()
    {
        return implicitTransaction;
    }

    public Duration getTxTimeout()
    {
        return txTimeout;
    }

    public Map<String,Object> getTxMetadata()
    {
        return txMetadata;
    }

    public void setMetaData( Map<String,Object> txMeta )
    {
        txMetadata = txMeta;
    }

    public RoutingContext getRoutingContext()
    {
        return routingContext;
    }
}
