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

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.api.index.StoreScan.NO_EXTERNAL_UPDATES;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockManager;

class IndexedStoreScanTest {
    @Test
    void shouldCloseLockClientOnClose() {
        // given
        var locks = mock(LockManager.Client.class);
        var delegate = mock(StoreScan.class);

        // when
        try (var storeScan = new IndexedStoreScan(locks, delegate)) {
            storeScan.run(NO_EXTERNAL_UPDATES);
        }

        // then
        var inOrder = inOrder(locks, delegate);
        inOrder.verify(delegate).run(NO_EXTERNAL_UPDATES);
        inOrder.verify(locks).close();
    }
}
