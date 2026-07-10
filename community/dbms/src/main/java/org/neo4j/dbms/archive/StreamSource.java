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
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.util.Preconditions;

public record StreamSource<T>(
        T base,
        BiFunction<T, Integer, T> mapper,
        ThrowingFunction<T, InputStream, IOException> reader,
        MutableInt counter) {
    public InputStream next() throws IOException {
        int version = counter.getAndIncrement();
        if (version == 0) {
            return reader.apply(base);
        } else {
            var newT = mapper.apply(base, version);
            return reader.apply(newT);
        }
    }

    /**
     * Lists all sibling files of the archive
     *
     * @param numSiblingParts the number of data parts beside the base, i.e. {@code 0} for an unsplit archive and
     *                        {@link CombiningInputStream#numParts} for a split one
     */
    public List<T> siblings(int numSiblingParts) {
        Preconditions.checkArgument(numSiblingParts >= 0, "numSiblingParts must not be negative");
        List<T> result = new ArrayList<>(numSiblingParts + 1);
        for (int i = 1; i <= numSiblingParts; i++) {
            result.add(mapper.apply(base, i));
        }
        return result;
    }

    public static StreamSource<URI> siblingsOf(URI uri, ThrowingFunction<URI, InputStream, IOException> open) {
        return generic(uri, (u, v) -> URI.create(u + "." + v), open);
    }

    public static StreamSource<Path> siblingsOf(FileSystemAbstraction fs, Path base) {
        var parent = base.getParent();
        Preconditions.checkArgument(base.isAbsolute(), "base must have an absolute path");
        Preconditions.checkArgument(parent != null, "base must have a parent");
        SequentialFileNameHelper fnHelper =
                new SequentialFileNameHelper(parent, base.getFileName().toString());
        return generic(base, (p, v) -> fnHelper.getFileForVersion(v), fs::openAsInputStream);
    }

    public static <T> StreamSource<T> generic(
            T t, BiFunction<T, Integer, T> newT, ThrowingFunction<T, InputStream, IOException> open) {
        return new StreamSource<>(t, newT, open, new MutableInt(0));
    }
}
