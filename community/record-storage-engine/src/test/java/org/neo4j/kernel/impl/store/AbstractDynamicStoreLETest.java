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
package org.neo4j.kernel.impl.store;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.OpenOption;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.PageCacheOpenOptions;

public class AbstractDynamicStoreLETest extends AbstractDynamicStoreTest {

    @BeforeEach
    @Override
    void before() throws IOException {
        try (StoreChannel channel = fs.write(storeFile)) {
            var buffer = ByteBuffers.allocate(pageCache.pageSize(), getByteOrder(), INSTANCE);
            // keep reserved bytes as zeros
            buffer.position(pageCache.pageReservedBytes(getOpenOptions()));
            buffer.putInt(BLOCK_SIZE);
            while (buffer.hasRemaining()) {
                buffer.putInt((byte) 0);
            }
            buffer.flip();
            channel.writeAll(buffer);
        }
    }

    @Override
    protected ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.of(PageCacheOpenOptions.MULTI_VERSIONED);
    }
}
