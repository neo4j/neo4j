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
package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.NeoStores;

/**
 * After all relationships have been imported and indexes completed, any violating input has been
 * detected. This stage makes one pass over those violating relationship ids
 * and deletes from the store(s).
 * This must be done before the linking of the relationships - here it's assumed that they are not linked.
 */
public class DeleteViolatingRelationshipsStage extends Stage {
    public DeleteViolatingRelationshipsStage(
            Configuration config,
            LongIterator violatingRelationshipIds,
            NeoStores neoStore,
            DataImporter.Monitor storeMonitor,
            DataStatistics.Client client,
            CursorContextFactory contextFactory) {
        super("DELETE", null, config, 0);
        add(new DeleteViolatingRelationshipsStep(
                control(), config, violatingRelationshipIds, neoStore, storeMonitor, client, contextFactory));
    }
}
