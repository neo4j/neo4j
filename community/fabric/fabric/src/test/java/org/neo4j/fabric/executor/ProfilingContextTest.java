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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.FakeClock;

@ExtendWith(MockitoExtension.class)
@TestDirectoryExtension
class ProfilingContextTest {

    @Inject
    private TestDirectory directory;

    private final FakeClock clock = new FakeClock();

    @Mock
    private ExecutingQuery query;

    private ProfilingContext profilingContext;

    private Path profilesDir;

    @BeforeEach
    void beforeEach() throws IOException {
        directory.cleanDirectory("profiles");

        when(query.elapsedMillis()).thenReturn(1234L);
        when(query.rawQueryText()).thenReturn("Composite query");
        when(query.id()).thenReturn("id-123");

        profilesDir = directory.directory("profiles");

        profilingContext = new ProfilingContextImpl(query, profilesDir, clock);
    }

    @Test
    void profileHandling() throws IOException {
        recordProfile(1, "Query 1", 10, "Profile 1");
        recordProfile(1, "Query 1", 30, "Profile 2");
        recordProfile(1, "Query 1", 20, "Profile 3");

        recordProfile(1, "Query 2", 12, "Profile 4");
        recordProfile(1, "Query 2", 32, "Profile 5");
        recordProfile(1, "Query 2", 22, "Profile 6");

        recordProfile(2, "Query 1", 11, "Profile 7");
        recordProfile(2, "Query 1", 31, "Profile 8");
        recordProfile(2, "Query 1", 21, "Profile 9");

        profilingContext.close();

        var profiles = Files.list(profilesDir).toList();
        assertThat(profiles).hasSize(1);
        var profileContent = Files.readString(profiles.get(0));

        var expected = """
                Composite Query:

                Composite query

                Composite query id: id-123

                Composite query duration: 1234ms

                Constituent queries and composite query snippets:

                Target: db1

                Query:

                Query 2

                Number of invocations: 3

                Longest duration: 32ms

                Profile of the longest duration:

                Profile 5

                Target: db2

                Query:

                Query 1

                Number of invocations: 3

                Longest duration: 31ms

                Profile of the longest duration:

                Profile 8

                Target: db1

                Query:

                Query 1

                Number of invocations: 3

                Longest duration: 30ms

                Profile of the longest duration:

                Profile 2

                """;

        assertThat(profileContent).isEqualToNormalizingNewlines(expected);
    }

    void recordProfile(int dbId, String query, long duration, String profileName) {
        var name = DatabaseIdFactory.from("db" + dbId, new UUID(0, dbId));
        var location = new Location.Local(-1, new DatabaseReferenceImpl.Internal(null, name, false));
        var f = profilingContext.fragmentStart(location, query);
        clock.forward(duration, TimeUnit.MILLISECONDS);
        f.finish(new Profile(profileName));
    }

    private static class Profile implements ExecutionPlanDescription {

        private final String name;

        Profile(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public String getName() {
            return "";
        }

        @Override
        public List<ExecutionPlanDescription> getChildren() {
            return List.of();
        }

        @Override
        public Map<String, Object> getArguments() {
            return Map.of();
        }

        @Override
        public Set<String> getIdentifiers() {
            return Set.of();
        }

        @Override
        public boolean hasProfilerStatistics() {
            return false;
        }

        @Override
        public ProfilerStatistics getProfilerStatistics() {
            return null;
        }
    }
}
