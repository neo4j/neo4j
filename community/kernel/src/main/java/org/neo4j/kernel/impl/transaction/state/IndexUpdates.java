/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

/**
 * Set of updates ({@link NodePropertyUpdate}) to apply to indexes.
 */
public interface IndexUpdates extends Iterable<NodePropertyUpdate>
{
    /**
     * Exposed since we infer index updates from physical commands AND contents in store.
     * This means that we cannot get to this information during recovery and so we merely need a way
     * to jot down which nodes needs to be reindexed after recovery.
     */
    void collectUpdatedNodeIds( PrimitiveLongSet target );
}
