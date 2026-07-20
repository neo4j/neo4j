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
package org.neo4j.internal.recordstorage;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.recordstorage.Command.RecordEnrichmentCommand;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.enrichment.CaptureMode;
import org.neo4j.storageengine.api.enrichment.Enrichment;
import org.neo4j.storageengine.api.enrichment.EnrichmentCommand;
import org.neo4j.storageengine.api.enrichment.TxMetadata;
import org.neo4j.test.extension.RandomSupportExtension;

@RandomSupportExtension
public class LogCommandSerializationV5_0Test extends LogCommandSerializationV5Base {

    @RepeatedTest(10)
    void nodeCommandSerialization() throws IOException {
        testDoubleSerialization(Command.NodeCommand.class, createRandomNode());
    }

    private Command.NodeCommand createRandomNode() {
        var id = Math.abs(random.nextLong());
        var before = createRandomNodeRecord(id);
        var after = createRandomNodeRecord(id);
        return new Command.NodeCommand(writer(), before, after);
    }

    @RepeatedTest(10)
    void relationshipCommandSerialization() throws IOException {
        testDoubleSerialization(Command.RelationshipCommand.class, createRandomRelationship());
    }

    private Command.RelationshipCommand createRandomRelationship() {
        var id = Math.abs(random.nextLong());
        var before = createRandomRelationshipRecord(random, id);
        var after = createRandomRelationshipRecord(random, id);
        return new Command.RelationshipCommand(writer(), before, after);
    }

    @RepeatedTest(10)
    void propertyCommandSerialization() throws IOException {
        testDoubleSerialization(Command.PropertyCommand.class, createRandomProperty());
    }

    Command.PropertyCommand createRandomProperty() {
        var id = Math.abs(random.nextLong());
        var before = createRandomPropertyRecord(id);
        var after = createRandomPropertyRecord(id);
        return new Command.PropertyCommand(writer(), before, after);
    }

    @Test
    void enrichmentReadNotSupported() {
        try (var channel = new InMemoryClosableChannel()) {
            final var writer = channel.writer();
            writer.beginChecksumForWriting();
            writer.put(EnrichmentCommand.COMMAND_CODE);
            writer.putLong(13L);
            writer.putChecksum();

            assertThatThrownBy(() -> createReader().read(channel.reader(), EmptyMemoryTracker.INSTANCE))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Unsupported in this version");
        }
    }

    @Test
    void enrichmentWriteNotSupported() {
        final var metadata = TxMetadata.create(CaptureMode.FULL, "some.server", securityContext(), 42L);
        final var enrichment = mock(Enrichment.Write.class);
        when(enrichment.metadata()).thenReturn(metadata);

        try (var channel = new InMemoryClosableChannel()) {
            final var writer = writer();
            final var command = new RecordEnrichmentCommand(writer, enrichment);

            assertThatThrownBy(() -> writer.writeEnrichmentCommand(channel, command))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Unsupported in this version");
        }
    }

    @Test
    void shouldHandleReadingAnIncorrectNumberOfRecords() {
        try (var channel = new InMemoryClosableChannel()) {
            // given a wrong large number of records
            channel.put((byte) 0); // flags
            channel.putInt(Integer.MAX_VALUE); // numberOfRecords

            // when/then throw ReadPastEndException and don't OOM
            assertThatThrownBy(() -> LogCommandSerializationV5_0.readNodeRecord(1, channel))
                    .isInstanceOf(ReadPastEndException.class);
        }
    }

    @Override
    CommandReader createReader() {
        return LogCommandSerializationV5_0.INSTANCE;
    }

    @Override
    LogCommandSerialization writer() {
        return LogCommandSerializationV5_0.INSTANCE;
    }
}
