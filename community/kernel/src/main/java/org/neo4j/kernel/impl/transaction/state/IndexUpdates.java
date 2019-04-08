/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.impl.api.index.EntityCommandGrouper;
import org.neo4j.kernel.impl.transaction.command.Command;

/**
 * Set of updates ({@link IndexEntryUpdate}) to apply to indexes.
 */
public interface IndexUpdates extends Iterable<IndexEntryUpdate<SchemaDescriptor>>
{
    /**
     * Feeds updates raw material in the form of node/property commands, to create updates from.
     * @param nodeCommands node data
     * @param relationshipCommands relationship data
     */
    void feed( EntityCommandGrouper<Command.NodeCommand>.Cursor nodeCommands, EntityCommandGrouper<Command.RelationshipCommand>.Cursor relationshipCommands );

    boolean hasUpdates();
}
