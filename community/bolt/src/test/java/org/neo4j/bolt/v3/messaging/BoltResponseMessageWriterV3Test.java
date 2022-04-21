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
package org.neo4j.bolt.v3.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.messaging.BoltResponseMessage.IGNORED;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.map;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackOutput;
import org.neo4j.bolt.v3.messaging.response.FailureMessage;
import org.neo4j.bolt.v3.messaging.response.IgnoredMessage;
import org.neo4j.bolt.v3.messaging.response.RecordMessage;
import org.neo4j.bolt.v3.messaging.response.SuccessMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

public class BoltResponseMessageWriterV3Test {
    @Test
    void shouldWriteRecordMessage() throws Exception {
        PackOutput output = mock(PackOutput.class);
        Neo4jPack.Packer packer = mock(Neo4jPack.Packer.class);

        var writer = newWriter(output, packer);

        writer.write(new RecordMessage(new AnyValue[] {longValue(42), stringValue("42")}));

        InOrder inOrder = inOrder(output, packer);
        inOrder.verify(output).beginMessage();
        inOrder.verify(packer).pack(longValue(42));
        inOrder.verify(packer).pack(stringValue("42"));
        inOrder.verify(output).messageSucceeded();
    }

    @Test
    void shouldWriteSuccessMessage() throws Exception {
        PackOutput output = mock(PackOutput.class);
        Neo4jPack.Packer packer = mock(Neo4jPack.Packer.class);

        var writer = newWriter(output, packer);

        MapValue metadata =
                map(new String[] {"a", "b", "c"}, new AnyValue[] {intValue(1), stringValue("2"), date(2010, 02, 02)});
        writer.write(new SuccessMessage(metadata));

        InOrder inOrder = inOrder(output, packer);
        inOrder.verify(output).beginMessage();
        inOrder.verify(packer).pack(metadata);
        inOrder.verify(output).messageSucceeded();
    }

    @Test
    void shouldWriteFailureMessage() throws Exception {
        PackOutput output = mock(PackOutput.class);
        Neo4jPack.Packer packer = mock(Neo4jPack.Packer.class);

        var writer = newWriter(output, packer);

        Status.Transaction errorStatus = Status.Transaction.DeadlockDetected;
        String errorMessage = "Hi Deadlock!";
        writer.write(new FailureMessage(errorStatus, errorMessage));

        InOrder inOrder = inOrder(output, packer);
        inOrder.verify(output).beginMessage();
        inOrder.verify(packer).pack(errorStatus.code().serialize());
        inOrder.verify(packer).pack(errorMessage);
        inOrder.verify(output).messageSucceeded();
    }

    @Test
    void shouldWriteIgnoredMessage() throws Exception {
        PackOutput output = mock(PackOutput.class);
        Neo4jPack.Packer packer = mock(Neo4jPack.Packer.class);

        var writer = newWriter(output, packer);

        writer.write(IgnoredMessage.IGNORED_MESSAGE);

        InOrder inOrder = inOrder(output, packer);
        inOrder.verify(output).beginMessage();
        inOrder.verify(packer).packStructHeader(0, IGNORED.signature());
        inOrder.verify(output).messageSucceeded();
    }

    @Test
    void shouldFlush() throws Exception {
        PackOutput output = mock(PackOutput.class);
        Neo4jPack.Packer packer = mock(Neo4jPack.Packer.class);

        var writer = newWriter(output, packer);

        writer.flush();

        verify(output).flush();
    }

    @Test
    void shouldNotifyOutputAboutFailedRecordMessage() throws Exception {
        PackOutput output = mock(PackOutput.class);
        Neo4jPack.Packer packer = mock(Neo4jPack.Packer.class);
        IOException error = new IOException("Unable to pack 42");
        doThrow(error).when(packer).pack(longValue(42));

        var writer = newWriter(output, packer);

        var e = assertThrows(
                IOException.class,
                () -> writer.write(new RecordMessage(new AnyValue[] {stringValue("42"), longValue(42)})));
        assertEquals(error, e);

        InOrder inOrder = inOrder(output, packer);
        inOrder.verify(output).beginMessage();
        inOrder.verify(packer).pack(stringValue("42"));
        inOrder.verify(packer).pack(longValue(42));
        inOrder.verify(output).messageFailed();
    }

    @Test
    void shouldNotNotifyOutputWhenOutputItselfFails() throws Exception {
        PackOutput output = mock(PackOutput.class);
        Neo4jPack.Packer packer = mock(Neo4jPack.Packer.class);
        IOException error = new IOException("Unable to flush");
        doThrow(error).when(output).messageSucceeded();

        var writer = newWriter(output, packer);

        var e = assertThrows(
                IOException.class, () -> writer.write(new RecordMessage(new AnyValue[] {longValue(1), longValue(2)})));
        assertEquals(error, e);

        InOrder inOrder = inOrder(output, packer);
        inOrder.verify(output).beginMessage();
        inOrder.verify(packer).pack(longValue(1));
        inOrder.verify(packer).pack(longValue(2));
        inOrder.verify(output).messageSucceeded();

        verify(output, never()).messageFailed();
    }

    /**
     * Asserts that large values aren't passed directly to the log provider as this may lead to overflows when flushing the message.
     */
    @Test
    void shouldLimitLogOutputToSensibleSizes() throws IOException {
        PackOutput output = mock(PackOutput.class);

        Neo4jPack.Packer packer = mock(Neo4jPack.Packer.class);
        IOException error = new IOException("Unable to flush");
        doThrow(error).when(packer).pack(ArgumentMatchers.any(AnyValue.class));

        AssertableLogProvider logProvider = new AssertableLogProvider();

        var writer = new BoltResponseMessageWriterV3(out -> packer, output, new SimpleLogService(logProvider));

        var listValue = VirtualValues.list();
        for (var i = 0; i < 1000; i++) {
            listValue = VirtualValues.list(listValue);
        }

        var testValue = listValue;
        var cause = assertThrows(IOException.class, () -> writer.consumeField(testValue));
        assertSame(error, cause);

        assertThat(logProvider)
                .forClass(BoltResponseMessageWriterV3.class)
                .containsMessagesOnce("Failed to write value");
        assertThat(logProvider)
                .forClass(BoltResponseMessageWriterV3.class)
                .doesNotContainMessageWithArguments(
                        "Failed to write value %s because: %s", listValue, cause.getMessage());
    }

    protected BoltResponseMessageWriter newWriter(PackOutput output, Neo4jPack.Packer packer) {
        return new BoltResponseMessageWriterV3(out -> packer, output, NullLogService.getInstance());
    }
}
