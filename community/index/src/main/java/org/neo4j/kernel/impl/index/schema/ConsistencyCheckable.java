/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.pagecache.context.CursorContextFactory;

public interface ConsistencyCheckable {
    /**
     * @return {@code true} if consistent, otherwise {@code false} and one or more issues reported to {@code reporterFactory}.
     */
    boolean consistencyCheck(
            ReporterFactory reporterFactory,
            CursorContextFactory contextFactory,
            int numThreads,
            ProgressMonitorFactory progressMonitorFactory);

    /**
     * Uses {@link ProgressMonitorFactory#NONE}.
     *
     * @see #consistencyCheck(ReporterFactory, CursorContextFactory, int, ProgressMonitorFactory)
     */
    default boolean consistencyCheck(
            ReporterFactory reporterFactory, CursorContextFactory contextFactory, int numThreads) {
        return consistencyCheck(reporterFactory, contextFactory, numThreads, ProgressMonitorFactory.NONE);
    }
}
