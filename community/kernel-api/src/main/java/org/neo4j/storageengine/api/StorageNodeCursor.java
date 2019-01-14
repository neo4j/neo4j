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
package org.neo4j.storageengine.api;

/**
 * Cursor over nodes and its data.
 */
public interface StorageNodeCursor extends StorageEntityScanCursor
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
     * NOTE the fact that this method is here means physical details about underlying storage leaks into this API.
     * However this method has to exist as long as the kernel API also exposes this. This needs to change at some point.
     *
     * @return reference for reading relationship groups, i.e. relationships split up by direction/type of the node this cursor currently is placed at.
     */
    long relationshipGroupReference();

    /**
     * @return reference for reading all relationships of the node this cursor currently is placed at.
     */
    long allRelationshipsReference();

    /**
     * A means of simplifying higher-level cursors which takes into consideration transaction-state.
     * This basically tells this cursor to be placed at nodeReference, even if it doesn't exist, such that
     * {@link #entityReference()} will return this reference on the next call.
     *
     * @param nodeReference the reference to be returned on the next {@link #entityReference()} call.
     */
    void setCurrent( long nodeReference );

    /**
     * NOTE the fact that this method is here means physical details about underlying storage leaks into this API.
     * However this method has to exist as long as the kernel API also exposes this. This needs to change at some point.
     *
     * @return whether or not this node is dense.
     */
    boolean isDense();
}
