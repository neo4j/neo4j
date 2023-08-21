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
package org.neo4j.kernel.impl.factory;

import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;

import java.util.function.Consumer;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;

public class FacadeKernelTransactionFactory implements KernelTransactionFactory {
    private final Config config;
    private final GraphDatabaseFacade facade;

    public FacadeKernelTransactionFactory(Config config, GraphDatabaseFacade facade) {
        this.config = config;
        this.facade = facade;
    }

    @Override
    public KernelTransaction beginKernelTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo connectionInfo,
            Consumer<Status> terminationCallback) {
        return facade.beginKernelTransaction(
                type,
                loginContext,
                connectionInfo,
                config.get(transaction_timeout).toMillis());
    }
}
