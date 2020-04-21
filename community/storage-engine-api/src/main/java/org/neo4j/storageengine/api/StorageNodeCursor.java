/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.storageengine.api;

/**
 * Cursor over nodes and its data.
 */
public interface StorageNodeCursor extends StorageEntityScanCursor<AllNodeScan>
{
    /**
     * @return label ids of the node this cursor currently is placed at.
     */
    long[] labels();

    /**
     * @return {@code true} if the node this cursor is placed at has the given {@code label}, otherwise {@code false}.
     */
    boolean hasLabel( int label );

    /**
     * @return reference for reading all relationships of the node this cursor currently is placed at.
     */
    long relationshipsReference();

    void relationships( StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection );

    int[] relationshipTypes();

    /**
     * Visits degrees, i.e. number of relationships, for relationships of the given {@code selection} and gives those degrees to the {@code mutator}.
     * @param selection {@link RelationshipSelection} to get degrees for.
     * @param mutator to given the degrees to.
     * @param allowFastDegreeLookup set to false if there's a need to do security checks, which require the lookup to be done with traversal.
     */
    void degrees( RelationshipSelection selection, Degrees.Mutator mutator, boolean allowFastDegreeLookup );

    /**
     * NOTE the fact that this method is here means physical details about underlying storage leaks into this API.
     * However this method has to exist as long as the kernel API also exposes this. This needs to change at some point.
     *
     * @return whether or not this node is dense.
     */
    boolean supportsFastDegreeLookup();
}
