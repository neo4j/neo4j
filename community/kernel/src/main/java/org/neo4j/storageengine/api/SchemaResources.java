/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.storageengine.api;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Interface for accessing schema resources.
 */
public interface SchemaResources extends AutoCloseable
{
    /**
     * Closes this statement so that the resources can be freed, the instance can still be reused.
     */
    @Override
    void close();

    /**
     * @return {@link LabelScanReader} capable of reading nodes for specific label ids.
     */
    LabelScanReader getLabelScanReader();

    /**
     * Returns an {@link IndexReader} for searching entity ids given property values. One reader is allocated
     * and kept per index throughout the life of a statement, making the returned reader repeatable-read isolation.
     * <p>
     * <b>NOTE:</b>
     * Reader returned from this method should not be closed. All such readers will be closed during {@link #close()}
     * of the current statement.
     *
     * @param index {@link IndexDescriptor} to get reader for.
     * @return {@link IndexReader} capable of searching entity ids given property values.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    IndexReader getIndexReader( IndexDescriptor index ) throws IndexNotFoundKernelException;

    /**
     * Returns an {@link IndexReader} for searching entity ids given property values. A new reader is allocated
     * every call to this method, which means that newly committed data since the last call to this method
     * will be visible in the returned reader.
     * <p>
     * <b>NOTE:</b>
     * It is caller's responsibility to close the returned reader.
     *
     * @param index {@link IndexDescriptor} to get reader for.
     * @return {@link IndexReader} capable of searching entity ids given property values.
     * @throws IndexNotFoundKernelException if no such index exists.
     */
    IndexReader getFreshIndexReader( IndexDescriptor index ) throws IndexNotFoundKernelException;
}
