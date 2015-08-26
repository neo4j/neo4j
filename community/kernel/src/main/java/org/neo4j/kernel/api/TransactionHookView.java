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
package org.neo4j.kernel.api;

import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.state.NodeState;
import org.neo4j.kernel.impl.api.state.RelationshipState;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;
import org.neo4j.kernel.impl.util.diffsets.ReadableRelationshipDiffSets;

/**
 * A read-only view of the changes a transaction makes, used by {@link org.neo4j.kernel.api.TransactionHook}.
 */
public interface TransactionHookView
{
    // TODO: For historical reasons, this currently exposes more than needed. When time permits, constrain this interface further

    /**
     * Returns all nodes that, in this tx, have had labelId removed.
     */
    ReadableDiffSets<Long> nodesWithLabelChanged( int labelId );

    /**
     * Returns nodes that have been added and removed in this tx.
     */
    ReadableDiffSets<Long> addedAndRemovedNodes();

    /**
     * Returns rels that have been added and removed in this tx.
     */
    ReadableRelationshipDiffSets<Long> addedAndRemovedRelationships();

    /**
     * Nodes that have had labels, relationships, or properties modified in this tx.
     */
    Iterable<NodeState> modifiedNodes();

    /**
     * Rels that have properties modified in this tx.
     */
    Iterable<RelationshipState> modifiedRelationships();

    boolean relationshipIsAddedInThisTx( long relationshipId );

    boolean relationshipIsDeletedInThisTx( long relationshipId );

    boolean nodeIsAddedInThisTx( long nodeId );

    boolean nodeIsDeletedInThisTx( long nodeId );

    /**
     * @return {@code true} if the relationship was visited in this state, i.e. if it was created
     * by this current transaction, otherwise {@code false} where the relationship might need to be
     * visited from the store.
     */
    <EX extends Exception> boolean relationshipVisit( long relId, RelationshipVisitor<EX> visitor ) throws EX;

}
