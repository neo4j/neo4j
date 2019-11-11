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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexDropper;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.LogProvider;

import static org.neo4j.kernel.impl.api.index.IndexPopulationFailure.failure;

public class FailedPopulatingIndexProxyFactory implements FailedIndexProxyFactory
{
    private final IndexDescriptor descriptor;
    private final IndexDropper indexDropper;
    private final String indexUserDescription;
    private final IndexStatisticsStore indexStatisticsStore;
    private final LogProvider logProvider;

    FailedPopulatingIndexProxyFactory( IndexDescriptor descriptor,
            IndexDropper indexDropper,
            String indexUserDescription,
            IndexStatisticsStore indexStatisticsStore,
            LogProvider logProvider )
    {
        this.descriptor = descriptor;
        this.indexDropper = indexDropper;
        this.indexUserDescription = indexUserDescription;
        this.indexStatisticsStore = indexStatisticsStore;
        this.logProvider = logProvider;
    }

    @Override
    public IndexProxy create( Throwable failure )
    {
        return new FailedIndexProxy( descriptor, indexUserDescription, indexDropper, failure( failure ),
                indexStatisticsStore, logProvider );
    }
}
