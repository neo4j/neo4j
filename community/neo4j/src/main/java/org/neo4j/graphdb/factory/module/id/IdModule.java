/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.HashMap;
import java.util.Map;
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

public class IdModule
{
    // This resided in RecordStorageEngine prior to 3.3
    private static final boolean ID_BUFFERING_FLAG = FeatureToggles.flag( IdModule.class, "safeIdBuffering", true );

    private Function<String, ? extends IdGeneratorFactory> idGeneratorFactoryProvider;
    private Function<String, IdController> idControllerFactory;

    private final IdTypeConfigurationProvider idTypeConfigurationProvider;
    private final IdReuseEligibility eligibleForIdReuse;

    IdModule( JobScheduler jobScheduler, Function<String,IdGeneratorFactory> idFactoryProvider,
            IdTypeConfigurationProvider idTypeConfigurationProvider, IdReuseEligibility eligibleForIdReuse, boolean useIdBuffering )
    {
        this.idTypeConfigurationProvider = idTypeConfigurationProvider;
        this.eligibleForIdReuse = eligibleForIdReuse;
        createIdComponents( jobScheduler, idFactoryProvider, useIdBuffering );
    }

    private void createIdComponents( JobScheduler jobScheduler,
            Function<String,? extends IdGeneratorFactory> idGeneratorFactoryProvider, boolean idBuffering )
    {
        Function<String,? extends IdGeneratorFactory> factoryProvider = idGeneratorFactoryProvider;
        if ( ID_BUFFERING_FLAG && idBuffering )
        {
            Function<String,BufferingIdGeneratorFactory> bufferingIdGeneratorFactory = new Function<String,BufferingIdGeneratorFactory>()
            {
                private final Map<String,BufferingIdGeneratorFactory> idGenerators = new HashMap<>();

                @Override
                public BufferingIdGeneratorFactory apply( String databaseName )
                {
                    return idGenerators.computeIfAbsent( databaseName,
                            s -> new BufferingIdGeneratorFactory( idGeneratorFactoryProvider.apply( databaseName ), eligibleForIdReuse,
                                    idTypeConfigurationProvider ) );
                }
            };
            idControllerFactory = databaseName -> createBufferedIdController( bufferingIdGeneratorFactory.apply( databaseName ), jobScheduler );
            factoryProvider = bufferingIdGeneratorFactory;
        }
        else
        {
            idControllerFactory = any -> createDefaultIdController();
        }
        this.idGeneratorFactoryProvider = factoryProvider;
    }

    private static BufferedIdController createBufferedIdController( BufferingIdGeneratorFactory idGeneratorFactory, JobScheduler scheduler )
    {
        return new BufferedIdController( idGeneratorFactory, scheduler );
    }

    private static DefaultIdController createDefaultIdController()
    {
        return new DefaultIdController();
    }

    public DatabaseIdContext createIdContext( String databaseName )
    {
        IdGeneratorFactory generatorFactory = idGeneratorFactoryProvider.apply( databaseName );
        IdController idController = idControllerFactory.apply( databaseName );
        return new DatabaseIdContext( generatorFactory, idController );
    }
}
