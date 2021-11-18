/*
 * Copyright (c) "Neo4j"
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

import java.time.Clock;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.cypher.internal.CommunityCompilerFactory;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.CypherRuntimeConfiguration;
import org.neo4j.cypher.internal.LastCommittedTxIdProvider;
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory;
import org.neo4j.cypher.internal.cache.CypherQueryCaches;
import org.neo4j.cypher.internal.cache.CypherQueryCaches$;
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory;
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration;
import org.neo4j.cypher.internal.config.CypherConfiguration;
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
                                                   CypherRuntimeConfiguration runtimeConfig,
                                                   CypherQueryCaches queryCaches )
    {
        return new CommunityCompilerFactory( queryService, spi.monitors(), spi.logProvider(), plannerConfig, runtimeConfig, queryCaches );
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
        CaffeineCacheFactory cacheFactory = makeCacheFactory( spi );
        Clock clock = Clock.systemUTC();

        CypherQueryCaches queryCaches = makeCypherQueryCaches( spi, queryService, cypherConfig, cacheFactory, clock );
        CompilerFactory compilerFactory = makeCompilerFactory( queryService, spi, plannerConfig, runtimeConfig, queryCaches );

        if ( isSystemDatabase )
        {
            CypherPlannerConfiguration innerPlannerConfig = CypherPlannerConfiguration.fromCypherConfiguration( cypherConfig, spi.config(), false );
            CypherQueryCaches innerQueryCaches = makeCypherQueryCaches( spi, queryService, cypherConfig, cacheFactory, clock );
            CompilerFactory innerCompilerFactory = makeCompilerFactory( queryService, spi, innerPlannerConfig, runtimeConfig, innerQueryCaches );
            return new SystemExecutionEngine( queryService, innerQueryCaches, spi.logProvider(), compilerFactory, innerCompilerFactory );
        }
        else if ( spi.config().get( GraphDatabaseInternalSettings.snapshot_query ) )
        {
            return new SnapshotExecutionEngine( queryService, spi.config(), queryCaches, spi.logProvider(), compilerFactory );
        }
        else
        {
            return new ExecutionEngine( queryService, queryCaches, spi.logProvider(), compilerFactory );
        }
    }

    private CypherQueryCaches makeCypherQueryCaches( SPI spi, GraphDatabaseCypherService queryService, CypherConfiguration cypherConfig,
                                                    CaffeineCacheFactory cacheFactory, Clock clock )
    {
        return new CypherQueryCaches(
                new CypherQueryCaches.Config( cypherConfig ),
                new LastCommittedTxIdProvider( queryService ),
                cacheFactory,
                clock,
                spi.monitors(),
                spi.logProvider()
        );
    }

    private static CaffeineCacheFactory makeCacheFactory( SPI spi )
    {
        var monitoredExecutor = spi.jobScheduler().monitoredJobExecutor( Group.CYPHER_CACHE );
        return new ExecutorBasedCaffeineCacheFactory( job -> monitoredExecutor.execute( systemJob( "Query plan cache maintenance" ), job ) );
    }
}
