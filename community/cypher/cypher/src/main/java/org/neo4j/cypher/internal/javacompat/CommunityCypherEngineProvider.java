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
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.cypher.internal.CommunityCompilerFactory;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.config.CypherConfiguration;
import org.neo4j.cypher.internal.CypherRuntimeConfiguration;
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory;
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory;
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.Group;

import static org.neo4j.scheduler.JobMonitoringParams.systemJob;

public class CommunityCypherEngineProvider extends QueryEngineProvider
{
    @Override
    protected int enginePriority()
    {
        return 42; // Lower means better. The enterprise version will have a lower number
    }

    protected CompilerFactory makeCompilerFactory( GraphDatabaseCypherService queryService,
                                                   SPI spi,
                                                   CypherPlannerConfiguration plannerConfig,
                                                   CypherRuntimeConfiguration runtimeConfig )
    {
        return new CommunityCompilerFactory( queryService, spi.monitors(), makeCacheFactory(spi), spi.logProvider(), plannerConfig, runtimeConfig );
    }

    @Override
    protected QueryExecutionEngine createEngine( Dependencies deps,
                                                 GraphDatabaseAPI graphAPI,
                                                 boolean isSystemDatabase,
                                                 SPI spi )
    {
        GraphDatabaseCypherService queryService = new GraphDatabaseCypherService( graphAPI );
        deps.satisfyDependency( queryService );
        CypherConfiguration cypherConfig = CypherConfiguration.fromConfig( spi.config() );
        CypherPlannerConfiguration plannerConfig = CypherPlannerConfiguration.fromCypherConfiguration( cypherConfig, spi.config(), isSystemDatabase );
        CypherRuntimeConfiguration runtimeConfig = CypherRuntimeConfiguration.fromCypherConfiguration( cypherConfig );
        CompilerFactory compilerFactory = makeCompilerFactory( queryService, spi, plannerConfig, runtimeConfig );
        CaffeineCacheFactory cacheFactory = makeCacheFactory( spi );
        if ( isSystemDatabase )
        {
            CypherPlannerConfiguration innerPlannerConfig = CypherPlannerConfiguration.fromCypherConfiguration( cypherConfig, spi.config(), false );
            CommunityCompilerFactory innerCompilerFactory =
                    new CommunityCompilerFactory( queryService,spi.monitors(), cacheFactory, spi.logProvider(), innerPlannerConfig, runtimeConfig );
            return new SystemExecutionEngine( queryService, cacheFactory, spi.logProvider(), compilerFactory, innerCompilerFactory );
        }
        else if ( spi.config().get( GraphDatabaseInternalSettings.snapshot_query ) )
        {
            return new SnapshotExecutionEngine( queryService, spi.config(), cacheFactory, spi.logProvider(), compilerFactory );
        }
        else
        {
            return new ExecutionEngine( queryService, cacheFactory, spi.logProvider(), compilerFactory );
        }
    }

    private CaffeineCacheFactory makeCacheFactory( SPI spi )
    {
        var monitoredExecutor = spi.jobScheduler().monitoredJobExecutor( Group.CYPHER_CACHE );
        return new ExecutorBasedCaffeineCacheFactory( job -> monitoredExecutor.execute( systemJob( "Query plan cache maintenance" ), job ) );
    }
}
