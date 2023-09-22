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
package org.neo4j.storageengine.api.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.io.fs.BufferBackedChannel;
import org.neo4j.kernel.KernelVersion;
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
    void concurrentReadingOfBuffers() throws Throwable {
        // given
        final var metadata = TxMetadata.create(CaptureMode.DIFF, "some.server", securityContext(), 42L);

        // when
        final var capacity = 4096;
        try (var channel = new BufferBackedChannel(capacity)) {
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

            final var enrichment = Enrichment.Read.deserialize(
                    KernelVersion.VERSION_CDC_INTRODUCED, channel, EmptyMemoryTracker.INSTANCE);
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

    @ParameterizedTest
    @MethodSource("cdcKernelVersion")
    void memoryTracking(KernelVersion kernelVersion) throws IOException {
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
        try (var channel = new BufferBackedChannel(capacity)) {
            metadata.serialize(channel);

            final int enrichmentSize;
            if (kernelVersion == KernelVersion.VERSION_CDC_INTRODUCED) {
                enrichmentSize = (int) (capacity - channel.position() - (Integer.BYTES * 4));
                final var entitiesSize = random.nextInt(13, enrichmentSize / 4);
                final var detailsSize = random.nextInt(13, enrichmentSize / 4);
                final var changesSize = random.nextInt(13, enrichmentSize / 4);
                final var valuesSize = enrichmentSize - entitiesSize - detailsSize - changesSize;

                channel.putInt(entitiesSize)
                        .putInt(detailsSize)
                        .putInt(changesSize)
                        .putInt(valuesSize);
            } else {
                enrichmentSize = (int) (capacity - channel.position() - (Integer.BYTES * 5));
                final var entitiesSize = random.nextInt(13, enrichmentSize / 5);
                final var detailsSize = random.nextInt(13, enrichmentSize / 5);
                final var changesSize = random.nextInt(13, enrichmentSize / 5);
                final var valuesSize = random.nextInt(13, enrichmentSize / 5);
                final var metadataSize = enrichmentSize - entitiesSize - detailsSize - changesSize - valuesSize;

                channel.putInt(entitiesSize)
                        .putInt(detailsSize)
                        .putInt(changesSize)
                        .putInt(valuesSize)
                        .putInt(metadataSize);
            }

            channel.put(random.nextBytes(new byte[enrichmentSize]), 0, enrichmentSize)
                    .flip();

            try (var ignored = Enrichment.Read.deserialize(kernelVersion, channel, tracker)) {
                assertThat(sum(allocated))
                        .as("should have allocated the enrichment data")
                        .isEqualTo(enrichmentSize);
            }

            assertThat(sum(released))
                    .as("should have deallocated the enrichment data")
                    .isEqualTo(enrichmentSize);
        }
    }

    @Test
    void roundTrippingWithWriteEnrichment() throws IOException {
        // given
        final var tracker = EmptyMemoryTracker.INSTANCE;
        final var metadata = TxMetadata.create(CaptureMode.DIFF, "some.server", securityContext(), 42L);

        final var userMetadataSize = random.nextInt(0, 123);
        final var entitiesData = random.nextBytes(new byte[random.nextInt(1, 123)]);
        final var detailsData = random.nextBytes(new byte[random.nextInt(1, 123)]);
        final var changesData = random.nextBytes(new byte[random.nextInt(1, 123)]);
        final var valuesData = random.nextBytes(new byte[random.nextInt(1, 123)]);
        final var metadataData = random.nextBytes(new byte[userMetadataSize]);

        try (var entitiesChannel = new WriteEnrichmentChannel(tracker);
                var detailsChannel = new WriteEnrichmentChannel(tracker);
                var changesChannel = new WriteEnrichmentChannel(tracker);
                var valuesChannel = new WriteEnrichmentChannel(tracker);
                var metadataChannel = new WriteEnrichmentChannel(tracker)) {
            entitiesChannel.put(entitiesData);
            detailsChannel.put(detailsData);
            changesChannel.put(changesData);
            valuesChannel.put(valuesData);
            metadataChannel.put(metadataData);

            final var enrichment = Enrichment.Write.createV5_12(
                    metadata,
                    entitiesChannel.flip(),
                    detailsChannel.flip(),
                    changesChannel.flip(),
                    valuesChannel.flip(),
                    metadataChannel.flip());

            final var capacity = 4096;
            try (var channel = new BufferBackedChannel(capacity)) {
                // write it twice - checks that buffer is reset for each round (ex. clustering leader->members)
                enrichment.serialize(channel);
                enrichment.serialize(channel);
                channel.flip();

                final var enrichment1 = Enrichment.Read.deserialize(
                        KernelVersion.VERSION_CDC_USER_METADATA_INTRODUCED, channel, tracker);
                assertMetadata(enrichment1.metadata, metadata);
                assertBuffer(enrichment1.entities(), entitiesData);
                assertBuffer(enrichment1.entityDetails(), detailsData);
                assertBuffer(enrichment1.entityChanges(), changesData);
                assertBuffer(enrichment1.values(), valuesData);
                if (userMetadataSize == 0) {
                    assertThat(enrichment1.userMetadata()).isNotPresent();
                } else {
                    assertThat(enrichment1.userMetadata())
                            .isPresent()
                            .get()
                            .satisfies(buffer -> assertBuffer(buffer, metadataData));
                }

                final var enrichment2 = Enrichment.Read.deserialize(
                        KernelVersion.VERSION_CDC_USER_METADATA_INTRODUCED, channel, tracker);
                assertMetadata(enrichment2.metadata, metadata);
                assertBuffer(enrichment2.entities(), entitiesData);
                assertBuffer(enrichment2.entityDetails(), detailsData);
                assertBuffer(enrichment2.entityChanges(), changesData);
                assertBuffer(enrichment2.values(), valuesData);
                if (userMetadataSize == 0) {
                    assertThat(enrichment2.userMetadata()).isNotPresent();
                } else {
                    assertThat(enrichment2.userMetadata())
                            .isPresent()
                            .get()
                            .satisfies(buffer -> assertBuffer(buffer, metadataData));
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("cdcKernelVersion")
    void roundTrippingWithReadEnrichment(KernelVersion kernelVersion) throws IOException {
        // given
        final var tracker = EmptyMemoryTracker.INSTANCE;
        final var metadata = TxMetadata.create(CaptureMode.DIFF, "some.server", securityContext(), 42L);

        final var userMetadataSize = random.nextInt(0, 123);
        final var entitiesData = random.nextBytes(new byte[random.nextInt(1, 123)]);
        final var detailsData = random.nextBytes(new byte[random.nextInt(1, 123)]);
        final var changesData = random.nextBytes(new byte[random.nextInt(1, 123)]);
        final var valuesData = random.nextBytes(new byte[random.nextInt(0, 123)]);
        final var metadataData = random.nextBytes(new byte[userMetadataSize]);

        final var capacity = 4096;
        final Enrichment.Read enrichment;
        try (var channel = new BufferBackedChannel(capacity)) {
            metadata.serialize(channel);
            channel.putInt(entitiesData.length)
                    .putInt(detailsData.length)
                    .putInt(changesData.length)
                    .putInt(valuesData.length);

            if (kernelVersion == KernelVersion.VERSION_CDC_INTRODUCED) {
                channel.put(entitiesData, 0, entitiesData.length)
                        .put(detailsData, 0, detailsData.length)
                        .put(changesData, 0, changesData.length)
                        .put(valuesData, 0, valuesData.length);
            } else {
                channel.putInt(userMetadataSize)
                        .put(entitiesData, 0, entitiesData.length)
                        .put(detailsData, 0, detailsData.length)
                        .put(changesData, 0, changesData.length)
                        .put(valuesData, 0, valuesData.length)
                        .put(metadataData, 0, userMetadataSize);
            }

            enrichment = Enrichment.Read.deserialize(kernelVersion, channel.flip(), tracker);
        }

        try (var channel = new BufferBackedChannel(capacity)) {
            // write it twice - checks that buffer is reset for each round (ex. clustering leader->members)
            enrichment.serialize(channel);
            enrichment.serialize(channel);
            channel.flip();

            final var enrichment1 = Enrichment.Read.deserialize(kernelVersion, channel, tracker);
            assertMetadata(enrichment1.metadata, metadata);
            assertBuffer(enrichment1.entities(), entitiesData);
            assertBuffer(enrichment1.entityDetails(), detailsData);
            assertBuffer(enrichment1.entityChanges(), changesData);
            assertBuffer(enrichment1.values(), valuesData);
            if (kernelVersion == KernelVersion.VERSION_CDC_INTRODUCED || userMetadataSize == 0) {
                assertThat(enrichment1.userMetadata()).isNotPresent();
            } else {
                assertThat(enrichment1.userMetadata())
                        .isPresent()
                        .get()
                        .satisfies(buffer -> assertBuffer(buffer, metadataData));
            }

            final var enrichment2 = Enrichment.Read.deserialize(kernelVersion, channel, tracker);
            assertMetadata(enrichment2.metadata, metadata);
            assertBuffer(enrichment2.entities(), entitiesData);
            assertBuffer(enrichment2.entityDetails(), detailsData);
            assertBuffer(enrichment2.entityChanges(), changesData);
            assertBuffer(enrichment2.values(), valuesData);
            if (kernelVersion == KernelVersion.VERSION_CDC_INTRODUCED || userMetadataSize == 0) {
                assertThat(enrichment2.userMetadata()).isNotPresent();
            } else {
                assertThat(enrichment2.userMetadata())
                        .isPresent()
                        .get()
                        .satisfies(buffer -> assertBuffer(buffer, metadataData));
            }
        }
    }

    private static Stream<Arguments> cdcKernelVersion() {
        return Stream.of(
                Arguments.of(KernelVersion.VERSION_CDC_INTRODUCED),
                Arguments.of(KernelVersion.VERSION_CDC_USER_METADATA_INTRODUCED));
    }

    private static void assertMetadata(TxMetadata actual, TxMetadata expected) {
        assertThat(actual.captureMode()).isEqualTo(expected.captureMode());
        assertThat(actual.serverId()).isEqualTo(expected.serverId());
        assertThat(actual.subject().executingUser())
                .isEqualTo(expected.subject().executingUser());
        assertThat(actual.connectionInfo().protocol())
                .isEqualTo(expected.connectionInfo().protocol());
    }

    private static void assertBuffer(ByteBuffer actual, byte[] expected) {
        final var actualBytes = new byte[expected.length];
        actual.get(actualBytes);
        assertThat(actualBytes).isEqualTo(expected);
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
