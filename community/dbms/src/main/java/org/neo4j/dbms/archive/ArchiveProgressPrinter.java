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

import org.neo4j.graphdb.Resource;

public interface ArchiveProgressPrinter {
    ArchiveProgressPrinter EMPTY = new ArchiveProgressPrinter() {

        @Override
        public Resource startPrinting() {
            return Resource.EMPTY;
        }

        @Override
        public void reset() {}

        @Override
        public void maxBytes(long value) {}

        @Override
        public long maxBytes() {
            return 0;
        }

        @Override
        public void maxFiles(long value) {}

        @Override
        public long maxFiles() {
            return 0;
        }

        @Override
        public void beginFile() {}

        @Override
        public void printOnNextUpdate() {}

        @Override
        public void addBytes(long n) {}

        @Override
        public void endFile() {}

        @Override
        public void done() {}

        @Override
        public void printProgress() {}
    };

    Resource startPrinting();

    void reset();

    void maxBytes(long value);

    long maxBytes();

    void maxFiles(long value);

    long maxFiles();

    void beginFile();

    void printOnNextUpdate();

    void addBytes(long n);

    void endFile();

    void done();

    void printProgress();
}
