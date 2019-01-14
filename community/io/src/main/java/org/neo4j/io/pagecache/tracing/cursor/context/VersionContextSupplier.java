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
package org.neo4j.io.pagecache.tracing.cursor.context;

import java.util.function.LongSupplier;

/**
 * Supplier to create {@link VersionContext} used during version data read and write operations
 */
public interface VersionContextSupplier
{
    /**
     * Initialise current supplier with provider of last closed transaction ids
     * for future version context to be able to get version ids
     * @param lastClosedTransactionIdSupplier closed transaction id supplier.
     */
    void init( LongSupplier lastClosedTransactionIdSupplier );

    /**
     * Provide version context
     * @return instance of version context
     */
    VersionContext getVersionContext();

}
