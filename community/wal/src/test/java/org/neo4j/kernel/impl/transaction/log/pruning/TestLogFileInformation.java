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
package org.neo4j.kernel.impl.transaction.log.pruning;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;

/**
 * Test-side base implementation: subclasses override only the methods their scenario cares about.
 */
abstract class TestLogFileInformation implements LogFileInformation {
    private final long version;

    TestLogFileInformation(long version) {
        this.version = version;
    }

    @Override
    public long version() {
        return version;
    }

    @Override
    public Path path() {
        return Path.of("test-v" + version);
    }

    @Override
    public long getPreviousAppendIndexFromHeader() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFirstStartRecordTimestamp() throws IOException {
        throw new UnsupportedOperationException();
    }
}
