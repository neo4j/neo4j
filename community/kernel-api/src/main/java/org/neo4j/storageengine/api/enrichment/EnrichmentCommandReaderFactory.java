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

import java.io.IOException;
import java.util.function.Supplier;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.BaseCommandReader;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * A specific {@link CommandReaderFactory} that fully deserializes any enrichment data found in the transaction log
 * for downstream processing, ex. CDC
 */
public class EnrichmentCommandReaderFactory implements CommandReaderFactory {

    private final CommandReaderFactory commandReaderFactory;
    private final EnrichmentCommandFactory enrichmentCommandFactory;
    private final Supplier<MemoryTracker> memoryTracker;

    public EnrichmentCommandReaderFactory(
            CommandReaderFactory commandReaderFactory,
            EnrichmentCommandFactory enrichmentCommandFactory,
            Supplier<MemoryTracker> memoryTracker) {
        this.commandReaderFactory = commandReaderFactory;
        this.enrichmentCommandFactory = enrichmentCommandFactory;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public final CommandReader get(KernelVersion version) {
        final var reader = commandReaderFactory.get(version);
        if (reader instanceof BaseCommandReader baseReader) {
            return new BaseCommandReader() {
                @Override
                public StorageCommand read(byte commandType, ReadableChannel channel) throws IOException {
                    if (EnrichmentCommand.COMMAND_CODE == commandType) {
                        final var kernelVersion = kernelVersion();
                        final var enrichment = Enrichment.Read.deserialize(kernelVersion, channel, memoryTracker.get());
                        return enrichmentCommandFactory.create(kernelVersion, enrichment);
                    }

                    return baseReader.read(commandType, channel);
                }

                @Override
                public KernelVersion kernelVersion() {
                    return baseReader.kernelVersion();
                }
            };
        } else {
            // need to use the base version as we've already read the command byte
            return reader;
        }
    }
}
