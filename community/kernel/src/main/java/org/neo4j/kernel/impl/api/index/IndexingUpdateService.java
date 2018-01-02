/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.impl.transaction.state.IndexUpdates;

public interface IndexingUpdateService
{
    /**
     * Apply updates to the relevant indexes.
     */
    void apply( IndexUpdates updates ) throws IOException, IndexEntryConflictException;

    /**
     * Convert the updates for a node into index entry updates. This has to happen earlier than the actual
     * application of the updates to the indexes, so that the properties reflect the exact state of the
     * transaction.
     */
    Iterable<IndexEntryUpdate<LabelSchemaDescriptor>> convertToIndexUpdates( NodeUpdates nodeUpdates );
}
