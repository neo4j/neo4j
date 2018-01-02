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
package org.neo4j.kernel.monitoring.tracing;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;

/**
 * A TracerFactory determines the implementation of the tracers, that a database should use. Each implementation has
 * a particular name, which is given by the getImplementationName method, and is used for identifying it in the
 * {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration#tracer} setting.
 */
public interface TracerFactory
{
    /**
     * @return The name this implementation is identified by in the
     * {@link org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration#tracer} setting.
     */
    String getImplementationName();

    /**
     * Create a new PageCacheTracer instance.
     * @return The created instance.
     */
    PageCacheTracer createPageCacheTracer();

    /**
     * Create a new TransactionTracer instance.
     * @return The created instance.
     */
    TransactionTracer createTransactionTracer();

    /**
     * Create a new CheckPointTracer instance.
     * @return The created instance.
     */
    CheckPointTracer createCheckPointTracer();
}
