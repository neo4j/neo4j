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
package org.neo4j.kernel.impl.coreapi;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.RoutingInfo;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.core.TransactionalEntityFactory;
import org.neo4j.values.ElementIdMapper;

public interface InternalTransaction extends Transaction, TransactionalEntityFactory, ResourceMonitor {
    void setTransaction(KernelTransaction transaction);

    /**
     * Loop-hole to access underlying kernel transaction. This is intended to allow
     * gradual removal of the InternalTransaction interface.
     */
    KernelTransaction kernelTransaction();

    KernelTransaction.Type transactionType();

    SecurityContext securityContext();

    ClientConnectionInfo clientInfo();

    /**
     * Routing information provided by the client or {@code null} if not available.
     */
    RoutingInfo routingInfo();

    KernelTransaction.Revertable overrideWith(SecurityContext context);

    Optional<Status> terminationReason();

    void setMetaData(Map<String, Object> txMeta);

    void checkInTransaction();

    boolean isOpen();

    void terminate(Status reason);

    UUID getDatabaseId();

    String getDatabaseName();

    Entity validateSameDB(Entity entity);

    ElementIdMapper elementIdMapper();

    void commit(KernelTransaction.KernelTransactionMonitor monitor);
}
