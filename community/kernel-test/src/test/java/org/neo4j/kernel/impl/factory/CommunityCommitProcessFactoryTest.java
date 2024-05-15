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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.api.CommandCommitListeners;
import org.neo4j.kernel.impl.api.DatabaseTransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

class CommunityCommitProcessFactoryTest {
    @Test
    void createRegularCommitProcessWhenWritable() {
        var factory = new CommunityCommitProcessFactory();

        var commitProcess = factory.create(
                mock(TransactionAppender.class),
                mock(StorageEngine.class),
                writable(),
                false,
                CommandCommitListeners.NO_LISTENERS);

        assertThat(commitProcess).isInstanceOf(DatabaseTransactionCommitProcess.class);
    }

    @Test
    void createRegularCommitProcessWhenDynamicallyReadOnly() {
        var factory = new CommunityCommitProcessFactory();

        var commitProcess = factory.create(
                mock(TransactionAppender.class),
                mock(StorageEngine.class),
                readOnly(),
                false,
                CommandCommitListeners.NO_LISTENERS);

        assertThat(commitProcess).isInstanceOf(DatabaseTransactionCommitProcess.class);
    }
}
