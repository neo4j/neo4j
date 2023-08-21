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
import org.neo4j.storageengine.api.StorageCommand;

/**
 * The command for representing enrichment data in the transaction log. This command will have no interactions with the
 * stores.
 */
public interface EnrichmentCommand extends StorageCommand {

    byte COMMAND_CODE = (byte) 30;

    /**
     * @return the enrichment data captured during a transaction.
     */
    Enrichment enrichment();

    static Enrichment.Read extractForReading(EnrichmentCommand command) throws IOException {
        if (command.enrichment() instanceof Enrichment.Read read) {
            return read;
        }

        throw new IOException("The enrichment specified is not for reading");
    }

    static Enrichment.Write extractForWriting(EnrichmentCommand command) throws IOException {
        if (command.enrichment() instanceof Enrichment.Write write) {
            return write;
        }

        throw new IOException("The enrichment specified is not for writing");
    }
}
