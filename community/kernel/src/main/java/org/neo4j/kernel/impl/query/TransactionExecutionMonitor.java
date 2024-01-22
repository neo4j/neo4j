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
package org.neo4j.kernel.impl.query;

import org.neo4j.kernel.api.KernelTransaction;

public interface TransactionExecutionMonitor {
    void start(KernelTransaction tx);

    void commit(KernelTransaction tx);

    void rollback(KernelTransaction tx, Throwable failure);

    TransactionExecutionMonitor NO_OP = new TransactionExecutionMonitor() {
        @Override
        public void start(KernelTransaction tx) {}

        @Override
        public void commit(KernelTransaction tx) {}

        @Override
        public void rollback(KernelTransaction tx, Throwable failure) {}
    };
}
