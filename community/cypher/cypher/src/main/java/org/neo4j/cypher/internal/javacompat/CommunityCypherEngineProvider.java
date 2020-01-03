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

import org.neo4j.cypher.internal.CommunityCompilerFactory;
import org.neo4j.cypher.internal.CypherConfiguration;
import org.neo4j.cypher.internal.compatibility.CypherRuntimeConfiguration;
import org.neo4j.cypher.internal.compiler.v3_5.CypherPlannerConfiguration;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;

@Service.Implementation( QueryEngineProvider.class )
public class CommunityCypherEngineProvider extends QueryEngineProvider
{
    public CommunityCypherEngineProvider()
    {
        super( "cypher" );
    }

    @Override
    protected int enginePriority()
    {
        return 42; // Lower means better. The enterprise version will have a lower number
    }

    @Override
    protected QueryExecutionEngine createEngine( Dependencies deps, GraphDatabaseAPI graphAPI )
    {
        GraphDatabaseCypherService queryService = new GraphDatabaseCypherService( graphAPI );
        deps.satisfyDependency( queryService );

        DependencyResolver resolver = graphAPI.getDependencyResolver();
        LogService logService = resolver.resolveDependency( LogService.class );
        Monitors monitors = resolver.resolveDependency( Monitors.class );
        Config config = resolver.resolveDependency( Config.class );
        CypherConfiguration cypherConfig = CypherConfiguration.fromConfig( config );
        CypherPlannerConfiguration plannerConfig = cypherConfig.toCypherPlannerConfiguration( config );
        CypherRuntimeConfiguration runtimeConfig = cypherConfig.toCypherRuntimeConfiguration();
        LogProvider logProvider = logService.getInternalLogProvider();
        CommunityCompilerFactory compilerFactory =
                new CommunityCompilerFactory( queryService, monitors, logProvider, plannerConfig, runtimeConfig );
        deps.satisfyDependencies( compilerFactory );
        return createEngine( queryService, config, logProvider, compilerFactory );
    }

    private QueryExecutionEngine createEngine( GraphDatabaseCypherService queryService, Config config,
                                               LogProvider logProvider, CommunityCompilerFactory compilerFactory )
    {
        return config.get( GraphDatabaseSettings.snapshot_query ) ?
               snapshotEngine( queryService, config, logProvider, compilerFactory ) :
               standardEngine( queryService, logProvider, compilerFactory );
    }

    private SnapshotExecutionEngine snapshotEngine( GraphDatabaseCypherService queryService, Config config,
                                                    LogProvider logProvider, CommunityCompilerFactory compilerFactory )
    {
        return new SnapshotExecutionEngine( queryService, config, logProvider, compilerFactory );
    }

    private ExecutionEngine standardEngine( GraphDatabaseCypherService queryService, LogProvider logProvider,
                                            CommunityCompilerFactory compilerFactory )
    {
        return new ExecutionEngine( queryService, logProvider, compilerFactory );
    }
}
