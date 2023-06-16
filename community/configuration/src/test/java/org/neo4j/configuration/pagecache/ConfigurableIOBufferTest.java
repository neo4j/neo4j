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
package org.neo4j.configuration.pagecache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_flush_buffer_size_in_pages;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.memory.DefaultScopedMemoryTracker;
import org.neo4j.memory.LocalMemoryTracker;

class ConfigurableIOBufferTest {
    @Test
    void ioBufferEnabledByDefault() {
        var config = Config.defaults();
        try (ConfigurableIOBuffer ioBuffer = new ConfigurableIOBuffer(config, INSTANCE)) {
            assertTrue(ioBuffer.isEnabled());
        }
    }

    @Test
    void allocatedBufferShouldHavePageAlignedAddress() {
        var config = Config.defaults();
        try (ConfigurableIOBuffer ioBuffer = new ConfigurableIOBuffer(config, INSTANCE)) {
            assertThat(ioBuffer.getAddress() % PAGE_SIZE).isZero();
        }
    }

    @Test
    void bufferPoolMemoryRegisteredInMemoryPool() {
        var config = Config.defaults();
        var memoryTracker = new DefaultScopedMemoryTracker(INSTANCE);
        try (ConfigurableIOBuffer ioBuffer = new ConfigurableIOBuffer(config, memoryTracker)) {
            assertThat(memoryTracker.usedNativeMemory())
                    .isEqualTo(PAGE_SIZE * pagecache_flush_buffer_size_in_pages.defaultValue() + PAGE_SIZE);
        }
        assertThat(memoryTracker.usedNativeMemory()).isZero();
    }

    @Test
    void canTryToCloseBufferSeveralTimes() {
        var config = Config.defaults();
        var memoryTracker = new DefaultScopedMemoryTracker(INSTANCE);
        ConfigurableIOBuffer ioBuffer = new ConfigurableIOBuffer(config, memoryTracker);
        assertThat(memoryTracker.usedNativeMemory())
                .isEqualTo(PAGE_SIZE * pagecache_flush_buffer_size_in_pages.defaultValue() + PAGE_SIZE);

        ioBuffer.close();
        assertThat(memoryTracker.usedNativeMemory()).isZero();

        ioBuffer.close();
        assertThat(memoryTracker.usedNativeMemory()).isZero();

        ioBuffer.close();
        assertThat(memoryTracker.usedNativeMemory()).isZero();
    }

    @Test
    void bufferSizeCanBeConfigured() {
        int customPageSize = 2;
        var config = Config.defaults(pagecache_flush_buffer_size_in_pages, customPageSize);
        var memoryTracker = new DefaultScopedMemoryTracker(INSTANCE);
        try (ConfigurableIOBuffer ioBuffer = new ConfigurableIOBuffer(config, memoryTracker)) {
            assertThat(memoryTracker.usedNativeMemory()).isEqualTo(PAGE_SIZE * customPageSize + PAGE_SIZE);
        }
    }

    @Test
    void bufferCapacityLimit() {
        int customPageSize = 5;
        var config = Config.defaults(pagecache_flush_buffer_size_in_pages, customPageSize);
        var memoryTracker = new DefaultScopedMemoryTracker(INSTANCE);
        try (ConfigurableIOBuffer ioBuffer = new ConfigurableIOBuffer(config, memoryTracker)) {
            assertThat(memoryTracker.usedNativeMemory()).isEqualTo(PAGE_SIZE * customPageSize + PAGE_SIZE);

            assertTrue(ioBuffer.hasMoreCapacity(0, 1));
            assertTrue(ioBuffer.hasMoreCapacity(PAGE_SIZE, PAGE_SIZE));
            assertTrue(ioBuffer.hasMoreCapacity(PAGE_SIZE * 2, PAGE_SIZE));
            assertTrue(ioBuffer.hasMoreCapacity(PAGE_SIZE * 3, PAGE_SIZE));
            assertTrue(ioBuffer.hasMoreCapacity(PAGE_SIZE * 4, PAGE_SIZE));

            assertFalse(ioBuffer.hasMoreCapacity(PAGE_SIZE * 4, PAGE_SIZE + 1));
            assertFalse(ioBuffer.hasMoreCapacity(PAGE_SIZE * 5, PAGE_SIZE));
            assertFalse(ioBuffer.hasMoreCapacity(PAGE_SIZE * 6, PAGE_SIZE));
            assertFalse(ioBuffer.hasMoreCapacity(PAGE_SIZE * 7, PAGE_SIZE));
            assertFalse(ioBuffer.hasMoreCapacity(PAGE_SIZE * 8, PAGE_SIZE));
        }
    }

    @Test
    void allocationFailureMakesBufferDisabled() {
        int customPageSize = 5;
        var config = Config.defaults(pagecache_flush_buffer_size_in_pages, customPageSize);
        var memoryTracker = new PoisonedMemoryTracker();
        try (ConfigurableIOBuffer ioBuffer = new ConfigurableIOBuffer(config, memoryTracker)) {
            assertTrue(memoryTracker.isExceptionThrown());
            assertFalse(ioBuffer.isEnabled());
        }
    }

    private static class PoisonedMemoryTracker extends LocalMemoryTracker {
        private boolean exceptionThrown;

        @Override
        public void allocateNative(long bytes) {
            exceptionThrown = true;
            throw new RuntimeException("Poison");
        }

        public boolean isExceptionThrown() {
            return exceptionThrown;
        }
    }
}
