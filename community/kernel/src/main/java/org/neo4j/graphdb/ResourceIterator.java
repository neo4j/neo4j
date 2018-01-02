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
package org.neo4j.graphdb;

import java.util.Iterator;

/**
 * Closeable Iterator with associated resources.
 *
 * The associated resources are always released when the owning transaction is committed or rolled back.
 * The resource may also be released eagerly by explicitly calling {@link org.neo4j.graphdb.ResourceIterator#close()}
 * or by exhausting the iterator.
 *
 * @param <T> type of values returned by this Iterator
 *
 * @see ResourceIterable
 * @see Transaction
 */
public interface ResourceIterator<T> extends Iterator<T>, Resource
{
    /**
     * Close the iterator early, freeing associated resources
     *
     * It is an error to use the iterator after this has been called.
     */
    @Override
    void close();
}
