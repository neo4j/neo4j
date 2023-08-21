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
package org.neo4j.test;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public final class Unzip {
    private Unzip() {}

    public static Path unzip(Class<?> testClass, String resource, Path targetDirectory) throws IOException {
        InputStream source = testClass.getResourceAsStream(resource);
        if (source == null) {
            var path = Path.of(resource);
            if (Files.exists(path)) {
                source = Files.newInputStream(path, StandardOpenOption.READ);
            } else {
                throw new NoSuchFileException("Could not find resource '" + resource + "' to unzip");
            }
        }

        try (ZipInputStream zipStream = new ZipInputStream(source)) {
            ZipEntry entry;
            byte[] scratch = new byte[8096];
            while ((entry = zipStream.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    Files.createDirectories(targetDirectory.resolve(entry.getName()));
                } else {
                    try (OutputStream file =
                            new BufferedOutputStream(Files.newOutputStream(targetDirectory.resolve(entry.getName())))) {
                        int read;
                        while ((read = zipStream.read(scratch)) != -1) {
                            file.write(scratch, 0, read);
                        }
                    }
                }
                zipStream.closeEntry();
            }
        } finally {
            source.close();
        }
        return targetDirectory;
    }
}
