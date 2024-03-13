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
package org.neo4j.kernel;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.neo4j.common.DependencyResolver;
import org.neo4j.csv.reader.CharReadable;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

/*
 * This is a trimmed down version of GraphDatabaseService and GraphDatabaseAPI, limited to a subset of functions needed
 * by implementations of QueryExecutionEngine.
 */
public interface GraphDatabaseQueryService {
    DependencyResolver getDependencyResolver();

    /**
     * Begin new internal transaction with with default timeout.
     *
     * @param type transaction type
     * @param loginContext transaction login context
     * @return internal transaction
     */
    InternalTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext);

    /**
     * Begin new internal transaction with with default timeout.
     *
     * @param type transaction type
     * @param loginContext transaction login context
     * @param connectionInfo transaction connection info
     * @return internal transaction
     */
    InternalTransaction beginTransaction(
            KernelTransaction.Type type, LoginContext loginContext, ClientConnectionInfo connectionInfo);

    /**
     * Begin new internal transaction with specified timeout in milliseconds.
     *
     * @param type transaction type
     * @param loginContext transaction login context
     * @param connectionInfo transaction connection info
     * @param timeout transaction timeout
     * @param unit time unit of timeout argument
     * @return internal transaction
     */
    InternalTransaction beginTransaction(
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo connectionInfo,
            long timeout,
            TimeUnit unit);

    CharReadable validateURIAccess(SecurityContext securityContext, URI uri) throws URLAccessValidationError;
}
