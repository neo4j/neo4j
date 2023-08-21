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
package org.neo4j.io.pagecache.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
public class FallbackBlockSwapperIT {

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private RandomSupport random;

    @Test
    void swapOutSwapIn() throws IOException {
        var file = testDirectory.createFile("test");
        var data = random.nextBytes(new byte[100]);
        var swapper = new FallbackBlockSwapper(EmptyMemoryTracker.INSTANCE);

        // write some data to file
        var source = UnsafeUtil.allocateMemory(data.length, EmptyMemoryTracker.INSTANCE);
        try (var channel = fs.write(file)) {
            for (int i = 0; i < data.length; i++) {
                UnsafeUtil.putByte(source + (long) i, data[i]);
            }
            swapper.swapOut(channel, source, 0, data.length);
        } finally {
            UnsafeUtil.free(source, data.length, EmptyMemoryTracker.INSTANCE);
        }

        // read back the data into another location
        var target = UnsafeUtil.allocateMemory(data.length, EmptyMemoryTracker.INSTANCE);
        try (var channel = fs.read(file)) {
            swapper.swapIn(channel, target, 0, data.length);
            for (int i = 0; i < data.length; i++) {
                assertThat(UnsafeUtil.getByte(target + (long) i)).isEqualTo(data[i]);
            }
        } finally {
            UnsafeUtil.free(target, data.length, EmptyMemoryTracker.INSTANCE);
        }
    }
}
