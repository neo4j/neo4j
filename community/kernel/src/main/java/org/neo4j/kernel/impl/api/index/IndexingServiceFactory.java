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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingControllerFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.SchemaRule;

/**
 * Factory to create {@link IndexingService}
 */
public class IndexingServiceFactory
{
    private IndexingServiceFactory()
    {
    }

    public static IndexingService createIndexingService( Config config,
            JobScheduler scheduler,
            IndexProviderMap providerMap,
            IndexStoreView storeView,
            TokenNameLookup tokenNameLookup,
            Iterable<SchemaRule> schemaRules,
            LogProvider internalLogProvider,
            LogProvider userLogProvider,
            IndexingService.Monitor monitor,
            SchemaState schemaState,
            boolean readOnly )
    {
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( config );
        MultiPopulatorFactory multiPopulatorFactory = MultiPopulatorFactory.forConfig( config );
        IndexMapReference indexMapRef = new IndexMapReference();
        IndexSamplingControllerFactory factory =
                new IndexSamplingControllerFactory( samplingConfig, storeView, scheduler, tokenNameLookup, internalLogProvider );
        IndexSamplingController indexSamplingController = factory.create( indexMapRef );
        IndexProxyCreator proxySetup =
                new IndexProxyCreator( samplingConfig, storeView, providerMap, tokenNameLookup, internalLogProvider );

        return new IndexingService( proxySetup, providerMap, indexMapRef, storeView, schemaRules,
                indexSamplingController, tokenNameLookup, scheduler, schemaState,
                multiPopulatorFactory, internalLogProvider, userLogProvider, monitor, readOnly );
    }
}
