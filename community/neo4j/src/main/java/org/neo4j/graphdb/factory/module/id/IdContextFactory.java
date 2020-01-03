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

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.BufferedIdController;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.DefaultIdController;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.BufferingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdReuseEligibility;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.FeatureToggles;

public class IdContextFactory
{
     private static final boolean ID_BUFFERING_FLAG = FeatureToggles.flag( IdContextFactory.class, "safeIdBuffering", true );

    private final JobScheduler jobScheduler;
    private final Function<String,IdGeneratorFactory> idFactoryProvider;
    private final IdTypeConfigurationProvider idTypeConfigurationProvider;
    private final IdReuseEligibility eligibleForIdReuse;
    private final Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper;

    IdContextFactory( JobScheduler jobScheduler, Function<String,IdGeneratorFactory> idFactoryProvider,
            IdTypeConfigurationProvider idTypeConfigurationProvider, IdReuseEligibility eligibleForIdReuse,
            Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper )
    {
        this.jobScheduler = jobScheduler;
        this.idFactoryProvider = idFactoryProvider;
        this.idTypeConfigurationProvider = idTypeConfigurationProvider;
        this.eligibleForIdReuse = eligibleForIdReuse;
        this.factoryWrapper = factoryWrapper;
    }

    public DatabaseIdContext createIdContext( String databaseName )
    {
        return ID_BUFFERING_FLAG ? createBufferingIdContext( idFactoryProvider, jobScheduler, databaseName )
                                 : createDefaultIdContext( idFactoryProvider, databaseName );
    }

    private DatabaseIdContext createDefaultIdContext( Function<String,? extends IdGeneratorFactory> idGeneratorFactoryProvider,
            String databaseName )
    {
        return createIdContext( idGeneratorFactoryProvider.apply( databaseName ), createDefaultIdController() );
    }

    private DatabaseIdContext createBufferingIdContext( Function<String,? extends IdGeneratorFactory> idGeneratorFactoryProvider, JobScheduler jobScheduler,
            String databaseName )
    {
        IdGeneratorFactory idGeneratorFactory = idGeneratorFactoryProvider.apply( databaseName );
        BufferingIdGeneratorFactory bufferingIdGeneratorFactory =
                new BufferingIdGeneratorFactory( idGeneratorFactory, eligibleForIdReuse, idTypeConfigurationProvider );
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
