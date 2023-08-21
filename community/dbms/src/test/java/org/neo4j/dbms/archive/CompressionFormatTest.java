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
package org.neo4j.dbms.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.dbms.archive.StandardCompressionFormat.GZIP;
import static org.neo4j.dbms.archive.StandardCompressionFormat.ZSTD;
import static org.neo4j.dbms.archive.StandardCompressionFormat.selectCompressionFormat;
import static org.neo4j.internal.helpers.ProcessUtils.start;

import org.junit.jupiter.api.Test;

class CompressionFormatTest {
    @Test
    void shouldSelectZstdAsDefault() {
        assertEquals(ZSTD, selectCompressionFormat());
    }

    @Test
    void shouldFallbackToGzipWhenZstdFails() throws Exception {
        // this test runs in a separate process to avoid problems with any parallel execution and shared static states
        int expectedExitCode = 66;
        var process = start(CompressionFormatTest.class.getName(), Integer.toString(expectedExitCode));
        assertEquals(expectedExitCode, process.waitFor()); // using exitcode to verify execution of correct function
    }

    public static void main(String[] args) {
        int exitCode = Integer.parseInt(args[0]);
        System.setProperty("os.arch", "foo"); // sabotage ZSTD loading
        StandardCompressionFormat format = selectCompressionFormat();
        assertEquals(GZIP, format, String.format("Should fallback to %s when %s fails", GZIP.name(), ZSTD.name()));
        System.exit(exitCode);
    }
}
