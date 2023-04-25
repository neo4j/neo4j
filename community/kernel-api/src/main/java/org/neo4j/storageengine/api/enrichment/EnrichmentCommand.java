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

import java.io.IOException;
import java.util.Optional;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * The command for representing enrichment data in the transaction log. This command will have no interactions with the
 * stores.
 */
public interface EnrichmentCommand extends StorageCommand {

    byte COMMAND_CODE = (byte) 30;

    /**
     * @return the metadata describing the enriched transaction
     */
    TxMetadata metadata();

    /**
     * @return the enrichment data captured during a transaction. The value is optional as the data could potentially
     * be large and some scans for the transaction log will not require any of this data. For example, a
     * Change-Data-Capture processor would require this data whereas a processor to find all the start positions of a
     * transaction would not.
     */
    Optional<Enrichment> enrichment();

    static Enrichment.Read extractForReading(EnrichmentCommand command) throws IOException {
        return (Enrichment.Read) command.enrichment()
                .filter(Enrichment.Read.class::isInstance)
                .orElseThrow(() -> new IOException("The enrichment specified is for writing"));
    }

    static Enrichment.Write extractForWriting(EnrichmentCommand command) throws IOException {
        return (Enrichment.Write) command.enrichment()
                .filter(Enrichment.Write.class::isInstance)
                .orElseThrow(() -> new IOException("The enrichment specified is for reading"));
    }
}
