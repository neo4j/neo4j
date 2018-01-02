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
package org.neo4j.unsafe.impl.batchimport.input;

import org.neo4j.collection.primitive.PrimitiveLongIterator;

/**
 * Collects items and is {@link #close() closed} after any and all items have been collected.
 * The {@link Collector} is responsible for closing whatever closeable resource received from the importer.
 */
public interface Collector extends AutoCloseable
{
    void collectBadRelationship( InputRelationship relationship, Object specificValue );

    void collectDuplicateNode( Object id, long actualId, String group, String firstSource, String otherSource );

    void collectExtraColumns( final String source, final long row, final String value );

    int badEntries();

    /**
     * @return iterator of node ids that were found to be duplicates of already imported nodes.
     * Returned node ids was imported, but never used to connect any relationship to, and should
     * be deleted. Must be returned sorted in ascending id order.
     */
    PrimitiveLongIterator leftOverDuplicateNodesIds();

    /**
     * Flushes whatever changes to the underlying resource supplied from the importer.
     */
    @Override
    void close();
}
