/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.transaction;

import java.util.Map;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;

public class StatementLifecycleTransactionInfo {
    private final LoginContext loginContext;
    private final ClientConnectionInfo clientConnectionInfo;
    protected Map<String, Object> txMetadata;

    public StatementLifecycleTransactionInfo(
            LoginContext loginContext, ClientConnectionInfo clientConnectionInfo, Map<String, Object> txMetadata) {
        this.loginContext = loginContext;
        this.clientConnectionInfo = clientConnectionInfo;
        this.txMetadata = txMetadata;
    }

    public ClientConnectionInfo getClientConnectionInfo() {
        return clientConnectionInfo;
    }

    public LoginContext getLoginContext() {
        return loginContext;
    }

    public Map<String, Object> getTxMetadata() {
        return txMetadata;
    }
}
