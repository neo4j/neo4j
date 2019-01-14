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
package org.neo4j.kernel.impl.pagecache;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class PublishPageCacheTracerMetricsAfterStart extends LifecycleAdapter
{
    private final PageCursorTracerSupplier pageCursorTracerSupplier;

    public PublishPageCacheTracerMetricsAfterStart( PageCursorTracerSupplier pageCursorTracerSupplier )
    {
        this.pageCursorTracerSupplier = pageCursorTracerSupplier;
    }

    @Override
    public void start()
    {
        // This will be called in the final stages of starting up a database, and will report any paging tracer
        // events caused by the startup process in the starting thread.
        pageCursorTracerSupplier.get().reportEvents();
    }
}
