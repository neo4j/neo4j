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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.function.BiFunction;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.util.Preconditions;

public interface StreamSource {
    InputStream next() throws IOException;

    static StreamSource siblingsOf(URI uri, ThrowingFunction<URI, InputStream, IOException> open) {
        return generic(uri, (u, v) -> URI.create(u + "." + v), open);
    }

    static StreamSource siblingsOf(FileSystemAbstraction fs, Path base) {
        var parent = base.getParent();
        Preconditions.checkArgument(base.isAbsolute(), "base must have an absolute path");
        Preconditions.checkArgument(parent != null, "base must have a parent");
        SequentialFileNameHelper fnHelper =
                new SequentialFileNameHelper(parent, base.getFileName().toString());
        return generic(base, (p, v) -> fnHelper.getFileForVersion(v), fs::openAsInputStream);
    }

    static <T> StreamSource generic(
            T t, BiFunction<T, Integer, T> newT, ThrowingFunction<T, InputStream, IOException> open) {
        return new StreamSource() {
            // Generates the sequence (t, t.1, t.2,...)
            int current = 0;

            @Override
            public InputStream next() throws IOException {
                var version = current++;
                if (version == 0) {
                    return open.apply(t);
                } else {
                    return open.apply(newT.apply(t, version));
                }
            }
        };
    }
}
