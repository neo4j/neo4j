/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.test.hosts;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * A source for free localhost IP addresses on this machine
 */
public class HostAuthority {
    private static final HostProvider HOST_PROVIDER;

    static {
        try {
            var directory = Optional.ofNullable(System.getProperty("host.authority.directory"))
                    .map(Paths::get)
                    .orElse(Paths.get(System.getProperty("java.io.tmpdir")).resolve("hostAuthority"));
            Files.createDirectories(directory);
            var hostRepository = new HostRepository(directory, HostConstants.EPHEMERAL_HOST_MINIMUM);
            HOST_PROVIDER = new CoordinatingHostProvider(hostRepository);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static String allocateHost() {
        var trace = buildTrace();
        return hostToAddress(HOST_PROVIDER.getNextFreeHost(trace));
    }

    private static String buildTrace() {
        var outputStream = new ByteArrayOutputStream();

        try (var printWriter = new PrintWriter(outputStream)) {
            new Exception().printStackTrace(printWriter);
        }

        return outputStream.toString();
    }

    static String hostToAddress(long host) {
        return LongStream.of(
                        (host & 0xFF_00_00_00L) >> 24,
                        (host & 0x00_FF_00_00L) >> 16,
                        (host & 0x00_00_FF_00L) >> 8,
                        host & 0x00_00_00_FFL)
                .mapToObj(Long::toString)
                .collect(Collectors.joining("."));
    }
}
