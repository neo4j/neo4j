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
package org.neo4j.kernel.internal;

import java.nio.file.Path;
import java.util.function.Predicate;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;

public abstract class IndexFileFilter implements Predicate<Path> {
    private final Path indexRoot;

    public IndexFileFilter(Path storeDir) {
        this.indexRoot = IndexDirectoryStructure.baseSchemaIndexFolder(storeDir).toAbsolutePath();
    }

    public Path indexRoot() {
        return indexRoot;
    }

    protected Path schemaPath(Path path) {
        return indexRoot.relativize(path);
    }

    protected String provider(Path path) {
        return schemaPath(path).getName(0).toString();
    }

    @Override
    public boolean test(Path path) {
        return path.toAbsolutePath().startsWith(indexRoot) && schemaPath(path).getNameCount() != 0;
    }
}
