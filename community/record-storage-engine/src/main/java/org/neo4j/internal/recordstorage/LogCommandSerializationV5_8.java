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

import java.io.IOException;
import org.neo4j.internal.recordstorage.Command.RecordEnrichmentCommand;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.enrichment.Enrichment;

class LogCommandSerializationV5_8 extends LogCommandSerializationV5_7 {
    static final LogCommandSerializationV5_8 INSTANCE = new LogCommandSerializationV5_8();

    @Override
    public KernelVersion kernelVersion() {
        return KernelVersion.V5_8;
    }

    @Override
    protected Command readEnrichmentCommand(ReadableChannel channel) throws IOException {
        return new RecordEnrichmentCommand(
                this, Enrichment.Read.deserialize(kernelVersion(), channel, EmptyMemoryTracker.INSTANCE));
    }

    @Override
    public void writeEnrichmentCommand(WritableChannel channel, RecordEnrichmentCommand command) throws IOException {
        final var enrichment = command.enrichment();
        channel.put(NeoCommandType.ENRICHMENT_COMMAND);
        enrichment.serialize(channel);
    }
}
