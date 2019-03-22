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
package org.neo4j.graphdb.factory.module.id;

import java.util.function.Function;

import org.neo4j.internal.id.BufferedIdController;
import org.neo4j.internal.id.BufferingIdGeneratorFactory;
import org.neo4j.internal.id.DefaultIdController;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.FeatureToggles;

public class IdContextFactory
{
     private static final boolean ID_BUFFERING_FLAG = FeatureToggles.flag( IdContextFactory.class, "safeIdBuffering", true );

    private final JobScheduler jobScheduler;
    private final Function<DatabaseId,IdGeneratorFactory> idFactoryProvider;
    private final IdTypeConfigurationProvider idTypeConfigurationProvider;
    private final Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper;

    IdContextFactory( JobScheduler jobScheduler, Function<DatabaseId,IdGeneratorFactory> idFactoryProvider,
            IdTypeConfigurationProvider idTypeConfigurationProvider, Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper )
    {
        this.jobScheduler = jobScheduler;
        this.idFactoryProvider = idFactoryProvider;
        this.idTypeConfigurationProvider = idTypeConfigurationProvider;
        this.factoryWrapper = factoryWrapper;
    }

    public DatabaseIdContext createIdContext( DatabaseId databaseId )
    {
        return ID_BUFFERING_FLAG ? createBufferingIdContext( idFactoryProvider, jobScheduler, databaseId )
                                 : createDefaultIdContext( idFactoryProvider, databaseId );
    }

    private DatabaseIdContext createDefaultIdContext( Function<DatabaseId,? extends IdGeneratorFactory> idGeneratorFactoryProvider,
            DatabaseId databaseId )
    {
        return createIdContext( idGeneratorFactoryProvider.apply( databaseId ), createDefaultIdController() );
    }

    private DatabaseIdContext createBufferingIdContext( Function<DatabaseId,? extends IdGeneratorFactory> idGeneratorFactoryProvider, JobScheduler jobScheduler,
            DatabaseId databaseId )
    {
        IdGeneratorFactory idGeneratorFactory = idGeneratorFactoryProvider.apply( databaseId );
        BufferingIdGeneratorFactory bufferingIdGeneratorFactory =
                new BufferingIdGeneratorFactory( idGeneratorFactory, idTypeConfigurationProvider );
        BufferedIdController bufferingController = createBufferedIdController( bufferingIdGeneratorFactory, jobScheduler );
        return createIdContext( bufferingIdGeneratorFactory, bufferingController );
    }

    private DatabaseIdContext createIdContext( IdGeneratorFactory idGeneratorFactory, IdController idController )
    {
        return new DatabaseIdContext( factoryWrapper.apply( idGeneratorFactory ), idController );
    }

    private static BufferedIdController createBufferedIdController( BufferingIdGeneratorFactory idGeneratorFactory, JobScheduler scheduler )
    {
        return new BufferedIdController( idGeneratorFactory, scheduler );
    }

    private static DefaultIdController createDefaultIdController()
    {
        return new DefaultIdController();
    }
}
