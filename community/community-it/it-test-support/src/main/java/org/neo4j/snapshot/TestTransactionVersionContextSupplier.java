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
package org.neo4j.snapshot;

import static java.util.Objects.requireNonNull;

import java.util.function.Function;
import org.neo4j.cypher.internal.javacompat.SnapshotExecutionEngine;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;

/**
 * A {@link VersionContextSupplier} and {@link VersionContextSupplier.Factory} that can have custom behaviour
 * injected in tests to verify the behavior of {@link SnapshotExecutionEngine}.
 */
public class TestTransactionVersionContextSupplier extends TransactionVersionContextSupplier {
    private final Function<String, TestVersionContext> supplier;
    private final NamedDatabaseId databaseId;

    public TestTransactionVersionContextSupplier(
            Function<String, TestVersionContext> supplier, NamedDatabaseId databaseId) {
        this.supplier = requireNonNull(supplier);
        this.databaseId = requireNonNull(databaseId);
    }

    @Override
    public VersionContext createVersionContext() {
        var ctx = supplier.apply(databaseId.name());
        return ctx == null ? super.createVersionContext() : ctx;
    }

    public static class Factory implements VersionContextSupplier.Factory {
        private volatile Function<String, TestVersionContext> wrappedContextSupplier;

        public void setTestVersionContextSupplier(Function<String, TestVersionContext> wrappedContextSupplier) {
            this.wrappedContextSupplier = wrappedContextSupplier;
        }

        /**
         * Method acts as a proxy for context suppliers which may be set after `create()` is called.
         */
        private TestVersionContext getVersionContext(String databaseName) {
            return wrappedContextSupplier == null ? null : wrappedContextSupplier.apply(databaseName);
        }

        @Override
        public VersionContextSupplier create(NamedDatabaseId databaseId) {
            return new TestTransactionVersionContextSupplier(this::getVersionContext, databaseId);
        }
    }
}
