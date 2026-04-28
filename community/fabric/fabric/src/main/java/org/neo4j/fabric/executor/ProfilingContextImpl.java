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
package org.neo4j.fabric.executor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.kernel.api.query.ExecutingQuery;

class ProfilingContextImpl implements ProfilingContext {

    private final Map<QueryKey, QueryRecord> queries = new ConcurrentHashMap<>();
    private final OutputStream outputStream;
    private final ExecutingQuery query;
    private final Clock clock;

    ProfilingContextImpl(ExecutingQuery query, Path outputDirectory, Clock clock) {
        this.query = query;
        this.clock = clock;
        var profileFile = createProfileFile(query, outputDirectory, clock);
        try {
            outputStream = Files.newOutputStream(profileFile);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Path createProfileFile(ExecutingQuery query, Path outputDirectory, Clock clock) {
        try {
            Files.createDirectories(outputDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        while (true) {
            // Let's use a format that does not use any characters that are problematic as filenames.
            // Especially Windows are quite restrictive. Since Windows disallow almost all reasonable
            // delimiters, let's play it safe with hyphens.
            var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS");
            var now = LocalDateTime.now(clock);
            var fileNameCandidate = formatter.format(now) + "-" + query.id();
            var path = outputDirectory.resolve(fileNameCandidate);
            try {
                return Files.createFile(path);
            } catch (FileAlreadyExistsException fileAlreadyExistsException) {
                // try again
                // query ID should make the name unique, but better safe than sorry
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public QueryFragment fragmentStart(Location location, String query) {
        var start = clock.millis();

        return profile -> {
            var duration = clock.millis() - start;
            queries.compute(new QueryKey(location, query), (k, v) -> {
                var count = 1;
                if (v != null) {
                    count += v.invocationCount;
                    if (v.duration > duration) {
                        return new QueryRecord(v.duration, v.profile, count);
                    }
                }

                var bytes = profile.toString().getBytes(StandardCharsets.UTF_8);
                return new QueryRecord(duration, bytes, count);
            });
        };
    }

    @Override
    public void close() {
        try {
            println("Composite Query:");
            println(query.rawQueryText());
            println("Composite query id: " + query.id());
            println("Composite query duration: " + query.elapsedMillis() + "ms");
            println("Constituent queries and composite query snippets:");
            queries.entrySet().stream()
                    .sorted(Comparator.<Map.Entry<QueryKey, QueryRecord>>comparingLong(e -> e.getValue().duration)
                            .reversed())
                    .forEach(e -> {
                        println("Target: " + getTargetString(e.getKey().location));
                        println("Query:");
                        println(e.getKey().query);
                        println("Number of invocations: " + e.getValue().invocationCount);
                        println("Longest duration: " + e.getValue().duration + "ms");
                        println("Profile of the longest duration:");
                        try {
                            outputStream.write(e.getValue().profile);
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                        println("");
                    });

            outputStream.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String getTargetString(Location location) {
        if (location instanceof Location.Remote.External external) {
            var namespace = external.locationNamespace().orElse("");
            return namespace + "." + external.locationName();
        }

        return location.getDatabaseName();
    }

    private void print(String str) {
        try {
            outputStream.write(str.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void println(String str) {
        print(str);
        print(System.lineSeparator());
        print(System.lineSeparator());
    }

    private record QueryRecord(long duration, byte[] profile, int invocationCount) {}

    private record QueryKey(Location location, String query) {}
}
