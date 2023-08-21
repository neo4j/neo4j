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
package org.neo4j.internal.id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.Race.throwing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.collection.trackable.HeapTrackingLongArrayList;
import org.neo4j.internal.id.BufferingIdGeneratorFactory.IdBuffer;
import org.neo4j.internal.id.IdController.TransactionSnapshot;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.RandomValues;

@ExtendWith(RandomExtension.class)
@TestDirectoryExtension
class DiskBufferedIdsTest {
    @Inject
    private RandomSupport random;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    private Path basePath;
    private DiskBufferedIds buffer;

    @BeforeEach
    void start() throws IOException {
        basePath = directory.file("buffer");
        buffer = new DiskBufferedIds(fs, basePath, INSTANCE, (int) ByteUnit.kibiBytes(500));
    }

    @AfterEach
    void stop() throws IOException {
        if (buffer != null) {
            buffer.close();
            buffer = null;
        }
    }

    @Test
    void shouldWriteAndReadChunks() throws IOException {
        // given
        var snapshot1 = randomSnapshot(random.randomValues());
        var buffers1 = randomBuffers(random.randomValues());
        var snapshot2 = randomSnapshot(random.randomValues());
        var buffers2 = randomBuffers(random.randomValues());

        // when
        buffer.write(snapshot1, copy(buffers1));
        buffer.write(snapshot2, copy(buffers2));

        // then
        var source = List.of(Pair.of(snapshot1, buffers1), Pair.of(snapshot2, buffers2))
                .iterator();
        buffer.read(new VerifyingReader(() -> source.hasNext() ? source.next() : null));
        assertThat(source.hasNext()).isFalse();
    }

    @Test
    void shouldNotAdvanceReaderOnNotEligibleForFree() throws IOException {
        // given
        List<Pair<TransactionSnapshot, List<IdBuffer>>> chunks = new ArrayList<>();
        for (var i = 0; i < 10; i++) {
            var chunk = Pair.of(randomSnapshot(random.randomValues()), randomBuffers(random.randomValues()));
            chunks.add(chunk);
            buffer.write(chunk.getLeft(), copy(chunk.getRight()));
        }

        // when
        var chunkIterator = chunks.iterator();
        var numRead = new MutableInt();
        while (chunkIterator.hasNext()) {
            buffer.read(new VerifyingReader(() -> chunkIterator.hasNext() ? chunkIterator.next() : null) {
                @Override
                public boolean startChunk(TransactionSnapshot snapshot) {
                    if (random.nextBoolean()) {
                        // Randomly claim that a chunk isn't eligible for reuse
                        return false;
                    }
                    return super.startChunk(snapshot);
                }

                @Override
                public void endChunk() {
                    super.endChunk();
                    numRead.increment();
                }
            });
        }

        // then
        assertThat(numRead.intValue()).isEqualTo(chunks.size());
    }

    @Test
    void shouldWriteAndReadChunksConcurrently() {
        // given
        var maxNumWritten = 500;
        var numWritten = new AtomicLong();
        var numRead = new AtomicLong();
        var race = new Race()
                .withEndCondition(() -> numWritten.get() >= maxNumWritten && numRead.get() == numWritten.get())
                .withRandomStartDelays();
        Deque<Pair<TransactionSnapshot, List<IdBuffer>>> queue = new ConcurrentLinkedDeque<>();
        race.addContestant(throwing(() -> {
            if (numWritten.get() >= maxNumWritten) {
                Thread.sleep(10);
                return;
            }
            var snapshot = randomSnapshot(random.randomValues());
            var idBuffers = randomBuffers(random.randomValues());
            queue.add(Pair.of(snapshot, idBuffers));
            buffer.write(snapshot, copy(idBuffers));
            numWritten.incrementAndGet();
        }));
        race.addContestant(throwing(() -> {
            // read
            buffer.read(new VerifyingReader(queue::poll) {
                @Override
                public void endChunk() {
                    super.endChunk();
                    numRead.incrementAndGet();
                }
            });
        }));
        race.goUnchecked();
    }

    @Test
    void shouldClearAnyExistingBufferFilesOnClose() throws IOException {
        // given
        for (int i = 0; i < 100; i++) {
            buffer.write(randomSnapshot(random.randomValues()), randomBuffers(random.randomValues()));
        }

        // when
        stop();

        // then
        assertThat(numberOfSegments()).isEqualTo(0);
    }

    @Test
    void shouldClearAnyExistingBufferFilesOnStart() throws IOException {
        // given
        List<Path> segmentPaths = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            segmentPaths.add(buffer.segmentName(i));
        }
        stop();
        for (Path segmentPath : segmentPaths) {
            Files.write(segmentPath, random.nextBytes(new byte[random.nextInt(100, 10_000)]));
        }

        // when
        start();

        // then
        assertThat(numberOfSegments()).isEqualTo(1);
        buffer.read(new VisitorAdapter() {
            @Override
            public boolean startChunk(TransactionSnapshot snapshot) {
                throw new RuntimeException("Should not find anything");
            }
        });
    }

    @Test
    void shouldRemoveFullyReadSegments() throws IOException {
        // given
        int[] positions = new int[10];
        int currentNumberOfSegments = numberOfSegments();
        int previousNumberOfSegments = currentNumberOfSegments;
        for (int i = 0; previousNumberOfSegments <= positions.length; i++) {
            currentNumberOfSegments = numberOfSegments();
            if (currentNumberOfSegments > previousNumberOfSegments) {
                positions[previousNumberOfSegments - 1] = i;
                previousNumberOfSegments = currentNumberOfSegments;
            }
            buffer.write(randomSnapshot(random.randomValues()), randomBuffers(random.randomValues()));
        }

        // when
        MutableInt segment = new MutableInt();
        buffer.read(new VisitorAdapter() {
            private int numFullyRead;

            @Override
            public boolean startChunk(TransactionSnapshot snapshot) {
                if (numFullyRead == positions[segment.intValue()]) {
                    // Expect the previous segment to now have been removed
                    segment.increment();
                    assertThat(numberOfSegments()).isEqualTo(positions.length + 1 - segment.intValue());
                }
                return true;
            }

            @Override
            public void endChunk() {
                numFullyRead++;
            }
        });
        assertThat(segment.intValue()).isEqualTo(positions.length);
    }

    private int numberOfSegments() {
        try {
            return fs.listFiles(basePath.getParent(), entry -> entry.getFileName()
                            .toString()
                            .contains(basePath.getFileName().toString()))
                    .length;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<IdBuffer> randomBuffers(RandomValues random) {
        List<IdBuffer> buffers = new ArrayList<>();
        for (var idType : random.selection(TestIdType.values(), 1, TestIdType.values().length, false)) {
            // In this test we might as well use the TestIdType#ordinal() method for the internal ordinal ids.
            buffers.add(randomIdBuffer(idType.ordinal(), random));
        }
        return buffers;
    }

    private IdBuffer randomIdBuffer(int idTypeOrdinal, RandomValues random) {
        return new IdBuffer(idTypeOrdinal, randomIds(random));
    }

    private HeapTrackingLongArrayList randomIds(RandomValues random) {
        var list = HeapTrackingLongArrayList.newLongArrayList(INSTANCE);
        var numIds = random.intBetween(1, 10_000);
        for (int i = 0; i < numIds; i++) {
            list.add(random.nextLong(0xFFFFFFFFFFFFL));
        }
        return list;
    }

    private TransactionSnapshot randomSnapshot(RandomValues random) {
        return new TransactionSnapshot(random.nextLong(), random.nextLong(), random.nextLong());
    }

    private List<IdBuffer> copy(List<IdBuffer> buffers) {
        List<IdBuffer> copiedBuffers = new ArrayList<>();
        for (IdBuffer idBuffer : buffers) {
            copiedBuffers.add(new IdBuffer(idBuffer.idTypeOrdinal(), copy(idBuffer.ids())));
        }
        return copiedBuffers;
    }

    private HeapTrackingLongArrayList copy(HeapTrackingLongArrayList ids) {
        int size = ids.size();
        HeapTrackingLongArrayList copiedIds = HeapTrackingLongArrayList.newLongArrayList(size, INSTANCE);
        for (int i = 0; i < size; i++) {
            copiedIds.add(ids.get(i));
        }
        return copiedIds;
    }

    private static class VerifyingReader implements BufferedIds.BufferedIdVisitor {
        private final Supplier<Pair<TransactionSnapshot, List<IdBuffer>>> source;
        private TransactionSnapshot snapshot;
        private int currentIdTypeOrdinal;
        private HeapTrackingLongArrayList currentIdList;
        private List<IdBuffer> buffers;

        VerifyingReader(Supplier<Pair<TransactionSnapshot, List<IdBuffer>>> source) {
            this.source = source;
        }

        @Override
        public boolean startChunk(TransactionSnapshot snapshot) {
            this.snapshot = snapshot;
            buffers = new ArrayList<>();
            return true;
        }

        @Override
        public void startType(int idTypeOrdinal) {
            currentIdTypeOrdinal = idTypeOrdinal;
            currentIdList = HeapTrackingLongArrayList.newLongArrayList(INSTANCE);
        }

        @Override
        public void id(long id) {
            currentIdList.add(id);
        }

        @Override
        public void endType() {
            buffers.add(new IdBuffer(currentIdTypeOrdinal, currentIdList));
        }

        @Override
        public void endChunk() {
            var expected = source.get();
            assertThat(expected).isNotNull();
            assertThat(snapshot.currentSequenceNumber())
                    .isEqualTo(expected.getLeft().currentSequenceNumber());
            assertThat(snapshot.snapshotTimeMillis())
                    .isEqualTo(expected.getLeft().snapshotTimeMillis());
            assertThat(snapshot.lastCommittedTransactionId())
                    .isEqualTo(expected.getLeft().lastCommittedTransactionId());
            var expectedBuffers = expected.getRight().iterator();
            for (var buffer1 : buffers) {
                var expectedBuffer = expectedBuffers.next();
                assertThat(buffer1.idTypeOrdinal()).isEqualTo(expectedBuffer.idTypeOrdinal());
                var expectedIdIterator = expectedBuffer.ids().iterator();
                var idIterator = buffer1.ids().iterator();
                while (idIterator.hasNext()) {
                    assertThat(expectedIdIterator.hasNext()).isTrue();
                    assertThat(idIterator.next()).isEqualTo(expectedIdIterator.next());
                }
                assertThat(expectedIdIterator.hasNext()).isFalse();
            }
        }
    }

    private abstract static class VisitorAdapter implements BufferedIds.BufferedIdVisitor {
        @Override
        public boolean startChunk(TransactionSnapshot snapshot) {
            return true;
        }

        @Override
        public void startType(int idTypeOrdinal) {}

        @Override
        public void id(long id) {}

        @Override
        public void endType() {}

        @Override
        public void endChunk() {}
    }
}
