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
package org.neo4j.graphdb.factory.module.id;

import java.util.function.Function;

import org.neo4j.internal.id.BufferedIdController;
import org.neo4j.internal.id.BufferingIdGeneratorFactory;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.scheduler.JobScheduler;

public class IdContextFactory
{
    private final JobScheduler jobScheduler;
    private final Function<NamedDatabaseId,IdGeneratorFactory> idFactoryProvider;
    private final Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper;
    private final PageCacheTracer cacheTracer;

    IdContextFactory( JobScheduler jobScheduler, Function<NamedDatabaseId,IdGeneratorFactory> idFactoryProvider,
            Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper, PageCacheTracer cacheTracer )
    {
        this.jobScheduler = jobScheduler;
        this.idFactoryProvider = idFactoryProvider;
        this.factoryWrapper = factoryWrapper;
        this.cacheTracer = cacheTracer;
    }

    public DatabaseIdContext createIdContext( NamedDatabaseId namedDatabaseId )
    {
        return createBufferingIdContext( idFactoryProvider, jobScheduler, cacheTracer, namedDatabaseId );
    }

    private DatabaseIdContext createBufferingIdContext( Function<NamedDatabaseId,? extends IdGeneratorFactory> idGeneratorFactoryProvider,
            JobScheduler jobScheduler, PageCacheTracer cacheTracer, NamedDatabaseId namedDatabaseId )
    {
        IdGeneratorFactory idGeneratorFactory = idGeneratorFactoryProvider.apply( namedDatabaseId );
        BufferingIdGeneratorFactory bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory( idGeneratorFactory );
        BufferedIdController bufferingController = createBufferedIdController( bufferingIdGeneratorFactory, jobScheduler, cacheTracer, namedDatabaseId.name() );
        return createIdContext( bufferingIdGeneratorFactory, bufferingController );
    }

    private DatabaseIdContext createIdContext( IdGeneratorFactory idGeneratorFactory, IdController idController )
    {
        return new DatabaseIdContext( factoryWrapper.apply( idGeneratorFactory ), idController );
    }

    private static BufferedIdController createBufferedIdController( BufferingIdGeneratorFactory idGeneratorFactory, JobScheduler scheduler,
            PageCacheTracer cacheTracer, String databaseName )
    {
        return new BufferedIdController( idGeneratorFactory, scheduler, cacheTracer, databaseName );
    }
}
