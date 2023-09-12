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
package org.neo4j.index.internal.gbptree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.LoggingStructureWriteLog.TYPES;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.list.primitive.LongList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.index.internal.gbptree.LoggingStructureWriteLog.Type;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class LoggingStructureWriteLogTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    @Test
    void shouldWriteAndReadEvents() throws IOException {
        // given
        var path = directory.file("log");
        Race race;
        CopyOnWriteArrayList<List<Event>> generatedEvents;
        try (var log = new LoggingStructureWriteLog(fs, path, kibiBytes(500))) {
            race = new Race();
            generatedEvents = new CopyOnWriteArrayList<>();

            // when
            race.addContestants(
                    4,
                    c -> () -> {
                        var threadEvents = new ArrayList<Event>();
                        var rng = new Random(random.seed() + c);
                        for (var s = 0; s < 10; s++) {
                            var session = log.newSession();
                            for (var i = 0; i < 2_000; i++) {
                                var type = TYPES[rng.nextInt(TYPES.length)];
                                var event =
                                        switch (type) {
                                            case SPLIT, MERGE, SUCCESSOR -> new Event(
                                                    type,
                                                    randomGeneration(rng),
                                                    randomTreeNodeId(rng),
                                                    randomTreeNodeId(rng),
                                                    randomTreeNodeId(rng));
                                            case TREE_GROW, TREE_SHRINK, FREELIST -> new Event(
                                                    type, randomGeneration(rng), randomTreeNodeId(rng));
                                            case CHECKPOINT -> new Event(
                                                    type,
                                                    randomGeneration(rng),
                                                    randomGeneration(rng),
                                                    randomGeneration(rng));
                                        };
                                event.fire(session, log);
                                threadEvents.add(event);
                            }
                        }
                        generatedEvents.add(threadEvents);
                    },
                    1);
            race.goUnchecked();
        }

        // then read events and see that they're all there
        var expectedEvents = combine(generatedEvents);
        var actualEvents = readEvents(path);
        Collections.sort(expectedEvents);
        Collections.sort(actualEvents);
        assertThat(actualEvents).isEqualTo(expectedEvents);
    }

    private List<Event> readEvents(Path path) throws IOException {
        var readEvents = new ArrayList<Event>();
        LoggingStructureWriteLog.read(fs, path, new LoggingStructureWriteLog.Events() {
            @Override
            public void split(
                    long timeMillis,
                    long sessionId,
                    long generation,
                    long parentId,
                    long childId,
                    long createdChildId) {
                readEvents.add(new Event(Type.SPLIT, generation, parentId, childId, createdChildId));
            }

            @Override
            public void merge(
                    long timeMillis,
                    long sessionId,
                    long generation,
                    long parentId,
                    long childId,
                    long deletedChildId) {
                readEvents.add(new Event(Type.MERGE, generation, parentId, childId, deletedChildId));
            }

            @Override
            public void createSuccessor(
                    long timeMillis, long sessionId, long generation, long parentId, long oldId, long newId) {
                readEvents.add(new Event(Type.SUCCESSOR, generation, parentId, oldId, newId));
            }

            @Override
            public void addToFreeList(long timeMillis, long sessionId, long generation, long id) {
                readEvents.add(new Event(Type.FREELIST, generation, id));
            }

            @Override
            public void checkpoint(
                    long timeMillis,
                    long previousStableGeneration,
                    long newStableGeneration,
                    long newUnstableGeneration) {
                readEvents.add(new Event(
                        Type.CHECKPOINT, previousStableGeneration, newStableGeneration, newUnstableGeneration));
            }

            @Override
            public void growTree(long timeMillis, long sessionId, long generation, long createdRootId) {
                readEvents.add(new Event(Type.TREE_GROW, generation, createdRootId));
            }

            @Override
            public void shrinkTree(long timeMillis, long sessionId, long generation, long deletedRootId) {
                readEvents.add(new Event(Type.TREE_SHRINK, generation, deletedRootId));
            }
        });
        return readEvents;
    }

    private static List<Event> combine(List<List<Event>> events) {
        var combined = new ArrayList<Event>();
        events.forEach(combined::addAll);
        return combined;
    }

    private long randomTreeNodeId(Random rng) {
        return rng.nextLong(Integer.MAX_VALUE);
    }

    private long randomGeneration(Random rng) {
        return rng.nextLong(0xFFFF_FFFFFFFFL);
    }

    // Have data as list because equals/hashCode/toString works more naturally there than on arrays
    private record Event(Type type, LongList data) implements Comparable<Event> {
        Event(Type type, long... data) {
            this(type, LongLists.immutable.of(data));
        }

        @Override
        public int compareTo(Event o) {
            var typeComparison = type.compareTo(o.type);
            return typeComparison != 0 ? typeComparison : Arrays.compare(data.toArray(), o.data.toArray());
        }

        void fire(StructureWriteLog.Session session, StructureWriteLog log) {
            switch (type) {
                case SPLIT -> session.split(data.get(0), data.get(1), data.get(2), data.get(3));
                case MERGE -> session.merge(data.get(0), data.get(1), data.get(2), data.get(3));
                case SUCCESSOR -> session.createSuccessor(data.get(0), data.get(1), data.get(2), data.get(3));
                case FREELIST -> session.addToFreelist(data.get(0), data.get(1));
                case TREE_GROW -> session.growTree(data.get(0), data.get(1));
                case TREE_SHRINK -> session.shrinkTree(data.get(0), data.get(1));
                case CHECKPOINT -> log.checkpoint(data.get(0), data.get(1), data.get(2));
                default -> throw new UnsupportedOperationException(type.toString());
            }
        }
    }
}
