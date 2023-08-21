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
package org.neo4j.kernel.impl.context;

import static org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory.EMPTY_SNAPSHOT_FACTORY;

import java.util.Objects;
import org.neo4j.io.pagecache.context.OldestTransactionIdFactory;
import org.neo4j.io.pagecache.context.TransactionIdSnapshotFactory;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.context.VersionContextSupplier;

/**
 * {@link VersionContextSupplier} that supplier version context that should be used in a context of
 * transaction(Committing or reading).
 */
public class TransactionVersionContextSupplier implements VersionContextSupplier {
    private TransactionIdSnapshotFactory transactionIdSnapshotFactory = EMPTY_SNAPSHOT_FACTORY;
    private OldestTransactionIdFactory oldestIdFactory = OldestTransactionIdFactory.EMPTY_OLDEST_ID_FACTORY;

    @Override
    public void init(
            TransactionIdSnapshotFactory transactionIdSnapshotFactory,
            OldestTransactionIdFactory oldestTransactionIdFactory) {
        this.transactionIdSnapshotFactory = Objects.requireNonNull(transactionIdSnapshotFactory);
        this.oldestIdFactory = oldestTransactionIdFactory;
    }

    @Override
    public VersionContext createVersionContext() {
        var versionContext = new TransactionVersionContext(transactionIdSnapshotFactory, oldestIdFactory);
        versionContext.initRead();
        return versionContext;
    }
}
