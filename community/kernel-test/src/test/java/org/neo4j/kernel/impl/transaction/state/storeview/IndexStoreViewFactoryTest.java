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
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexProxyProvider;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockService;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.StorageEngine;

class IndexStoreViewFactoryTest {
    FullScanStoreView fullScanStoreView = mock(FullScanStoreView.class);
    LockService lockService = mock(LockService.class);
    InternalLogProvider logProvider = mock(InternalLogProvider.class);
    IndexProxyProvider indexProxies = mock(IndexProxyProvider.class);
    LockManager locks = mock(LockManager.class);

    @Test
    void shouldCreateIndexStoreView() {
        // Given
        var storageEngine = mock(StorageEngine.class);
        var factory = new IndexStoreViewFactory(
                Config.defaults(), storageEngine, locks, fullScanStoreView, lockService, logProvider);

        // When
        var indexStoreView = factory.createTokenIndexStoreView(indexProxies);

        // Then
        assertThat(indexStoreView.getClass()).isEqualTo(DynamicIndexStoreView.class);
    }
}
