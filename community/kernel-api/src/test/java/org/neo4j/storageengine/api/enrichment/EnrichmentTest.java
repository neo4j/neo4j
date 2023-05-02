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
package org.neo4j.storageengine.api.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.function.Supplier;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class EnrichmentTest {

    @Inject
    private RandomSupport random;

    @Test
    void readMetadataAndPastEnrichmentData() throws IOException {
        // given
        final var metadata = TxMetadata.create(CaptureMode.DIFF, "some.server", securityContext(), 42L);

        // when
        final var capacity = 1024;
        final var tailSize = 128;
        final var tail = random.nextBytes(new byte[tailSize]);
        final var tailBuffer = ByteBuffer.allocate(tailSize)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(tail)
                .flip();

        try (var channel = new ChannelBuffer(capacity)) {
            metadata.serialize(channel);

            final var enrichmentSize = (int) (capacity - channel.position() - tailSize - (Integer.BYTES * 4));
            final var entitiesSize = random.nextInt(13, enrichmentSize / 4);
            final var detailsSize = random.nextInt(13, enrichmentSize / 4);
            final var changesSize = random.nextInt(13, enrichmentSize / 4);
            final var valuesSize = enrichmentSize - entitiesSize - detailsSize - changesSize;
            final var enrichmentBuffer = ByteBuffer.allocate(enrichmentSize)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .put(random.nextBytes(new byte[enrichmentSize]))
                    .flip();

            // mimic the output of enrichment data
            channel.putInt(entitiesSize)
                    .putInt(detailsSize)
                    .putInt(changesSize)
                    .putInt(valuesSize)
                    .putAll(enrichmentBuffer)
                    // now add some extra data AFTER the enrichment
                    .putAll(tailBuffer)
                    .flip();

            final var metadata2 = Enrichment.readMetadataAndPastEnrichmentData(channel);
            assertThat(metadata2.captureMode()).isEqualTo(metadata.captureMode());
            assertThat(metadata2.serverId()).isEqualTo(metadata.serverId());
            assertThat(metadata2.subject().executingUser())
                    .isEqualTo(metadata.subject().executingUser());
            assertThat(metadata2.connectionInfo().protocol())
                    .isEqualTo(metadata.connectionInfo().protocol());

            assertThat(channel.position())
                    .as("should have read all of the enrichment data")
                    .isEqualTo(capacity - tailSize);
            final var actualTail = new byte[tailSize];
            channel.get(actualTail, tailSize);
            assertThat(actualTail)
                    .as("should read the rest of the channel OK AFTER the enrichment")
                    .isEqualTo(tail);
        }
    }

    @Test
    void concurrentReadingOfBuffers() throws Throwable {
        // given
        final var metadata = TxMetadata.create(CaptureMode.DIFF, "some.server", securityContext(), 42L);

        // when
        final var capacity = 4096;
        try (var channel = new ChannelBuffer(capacity)) {
            metadata.serialize(channel);

            final var enrichmentSize = (int) (capacity - channel.position() - (Integer.BYTES * 4));
            final var entitiesSize = random.nextInt(13, enrichmentSize / 4);
            final var detailsSize = random.nextInt(13, enrichmentSize / 4);
            final var changesSize = random.nextInt(13, enrichmentSize / 4);
            final var valuesSize = enrichmentSize - entitiesSize - detailsSize - changesSize;
            final var enrichmentBytes = random.nextBytes(new byte[enrichmentSize]);
            final var enrichmentBuffer = ByteBuffer.allocate(enrichmentSize)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .put(enrichmentBytes)
                    .flip();

            // mimic the output of enrichment data
            channel.putInt(entitiesSize)
                    .putInt(detailsSize)
                    .putInt(changesSize)
                    .putInt(valuesSize)
                    .putAll(enrichmentBuffer)
                    .flip();

            final var enrichment = Enrichment.Read.deserialize(channel, EmptyMemoryTracker.INSTANCE);
            @SuppressWarnings("unchecked")
            final var buffersWithSize = Lists.mutable.with(
                    Pair.<Integer, Supplier<ByteBuffer>>of(entitiesSize, enrichment::entities),
                    Pair.<Integer, Supplier<ByteBuffer>>of(detailsSize, enrichment::entityDetails),
                    Pair.<Integer, Supplier<ByteBuffer>>of(changesSize, enrichment::entityChanges),
                    Pair.<Integer, Supplier<ByteBuffer>>of(valuesSize, enrichment::values));

            // read the buffers in chunks to check that their positions can update independently
            final var chunkSize = 8;
            final Runnable chunkReaders = () -> {
                final var bytes = new byte[chunkSize];
                var pos = 0;
                for (var bufferWithSize : buffersWithSize) {
                    final var size = bufferWithSize.first();
                    final var buffer = bufferWithSize.other().get();

                    for (var i = 0; i < size; i += chunkSize) {
                        final var bytesToRead = Math.min(chunkSize, size - i);
                        buffer.get(bytes, 0, bytesToRead);

                        final var actual = bytesToRead == chunkSize ? bytes : Arrays.copyOfRange(bytes, 0, bytesToRead);
                        assertThat(actual).isEqualTo(Arrays.copyOfRange(enrichmentBytes, pos, pos + actual.length));
                        pos += actual.length;
                    }
                }
            };

            final var race = new Race();
            race.addContestants(chunkSize, chunkReaders);
            race.go();
        }
    }

    @Test
    void memoryTracking() throws IOException {
        // given
        final var allocated = ArgumentCaptor.forClass(Long.class);
        final var released = ArgumentCaptor.forClass(Long.class);
        final var tracker = mock(MemoryTracker.class);
        doNothing().when(tracker).allocateHeap(allocated.capture());
        doNothing().when(tracker).releaseHeap(released.capture());

        // given
        final var metadata = TxMetadata.create(CaptureMode.DIFF, "some.server", securityContext(), 42L);

        // when
        final var capacity = 2048;
        try (var channel = new ChannelBuffer(capacity)) {
            metadata.serialize(channel);

            final var enrichmentSize = (int) (capacity - channel.position() - (Integer.BYTES * 4));
            final var entitiesSize = random.nextInt(13, enrichmentSize / 4);
            final var detailsSize = random.nextInt(13, enrichmentSize / 4);
            final var changesSize = random.nextInt(13, enrichmentSize / 4);
            final var valuesSize = enrichmentSize - entitiesSize - detailsSize - changesSize;
            final var enrichmentBytes = random.nextBytes(new byte[enrichmentSize]);

            // mimic the output of enrichment data
            channel.putInt(entitiesSize)
                    .putInt(detailsSize)
                    .putInt(changesSize)
                    .putInt(valuesSize)
                    .put(enrichmentBytes, 0, enrichmentSize)
                    .flip();

            try (var ignored = Enrichment.Read.deserialize(channel, tracker)) {
                assertThat(sum(allocated))
                        .as("should have allocated the enrichment data")
                        .isEqualTo(enrichmentSize);
            }

            assertThat(sum(released))
                    .as("should have deallocated the enrichment data")
                    .isEqualTo(enrichmentSize);
        }
    }

    private static SecurityContext securityContext() {
        final var subject = subject();
        final var securityContext = mock(SecurityContext.class);
        when(securityContext.subject()).thenReturn(subject);
        when(securityContext.connectionInfo()).thenReturn(ClientConnectionInfo.EMBEDDED_CONNECTION);
        return securityContext;
    }

    private static AuthSubject subject() {
        final var authSubject = mock(AuthSubject.class);
        when(authSubject.executingUser()).thenReturn("freddy");
        return authSubject;
    }

    private static long sum(ArgumentCaptor<Long> captor) {
        return captor.getAllValues().stream().mapToLong(l -> l).sum();
    }
}
