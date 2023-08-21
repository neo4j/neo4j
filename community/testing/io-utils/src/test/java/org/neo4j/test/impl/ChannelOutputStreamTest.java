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
package org.neo4j.test.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class ChannelOutputStreamTest {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldStoreAByteAtBoundary() throws Exception {
        try (EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction()) {
            Path workFile = testDirectory.file("a");
            fs.mkdirs(testDirectory.homePath());
            OutputStream out = fs.openAsOutputStream(workFile, false);

            // When I write a byte[] that is larger than the internal buffer in
            // ChannelOutputStream..
            byte[] b = new byte[8097];
            b[b.length - 1] = 7;
            out.write(b);
            out.flush();

            // Then it should get cleanly written and be readable
            InputStream in = fs.openAsInputStream(workFile);
            in.skip(8096);
            assertEquals(7, in.read());
        }
    }
}
