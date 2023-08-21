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
package org.neo4j.test.extension;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteOrder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EphemeralTestDirectoryExtension
class ClassLevelTestDirectoryExtensionTestSupport {
    @Inject
    FileSystemAbstraction fs;

    @Inject
    TestDirectory testDirectory;

    private StoreChannel channel;

    @BeforeAll
    void setUp() throws IOException {
        channel = fs.write(testDirectory.createFile("f"));
    }

    @AfterAll
    void tearDown() throws IOException {
        channel.close();
    }

    @RepeatedTest(10)
    void writeToChannelManyTimes() throws IOException {
        // This will fail if the test directory is not initialised,
        // or if the file is deleted by the clearing of the test directory,
        // in between the runs.
        channel.writeAll(ByteBuffers.allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE));
    }
}
