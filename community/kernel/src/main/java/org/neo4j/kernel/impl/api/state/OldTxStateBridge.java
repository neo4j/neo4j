/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import org.neo4j.kernel.impl.api.DiffSets;

/**
 * Temporary anti-corruption while the old {@link org.neo4j.kernel.impl.core.TransactionState} class
 * still remains.
 */
public interface OldTxStateBridge
{
    /**
     * A diff set of nodes that have had the given property key and value added or removed/changed.
     */
    DiffSets<Long> getNodesWithChangedProperty( long propertyKey, Object value );

    void deleteNode( long nodeId );
}
