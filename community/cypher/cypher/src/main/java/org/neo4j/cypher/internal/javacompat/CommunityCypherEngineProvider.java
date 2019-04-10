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
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.CommunityCompilerFactory;
import org.neo4j.cypher.internal.CompilerFactory;
import org.neo4j.cypher.internal.CypherConfiguration;
import org.neo4j.cypher.internal.CypherRuntimeConfiguration;
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.values.virtual.MapValue;

@ServiceProvider
public class CommunityCypherEngineProvider extends QueryEngineProvider
{
    @Override
    public String getName()
    {
        return "cypher";
    }

    @Override
    protected int enginePriority()
    {
        return 42; // Lower means better. The enterprise version will have a lower number
    }

    protected CompilerFactory makeCompilerFactory( GraphDatabaseCypherService queryService, Monitors monitors, LogProvider logProvider,
            CypherPlannerConfiguration plannerConfig, CypherRuntimeConfiguration runtimeConfig )
    {
        return new CommunityCompilerFactory( queryService, monitors, logProvider, plannerConfig, runtimeConfig );
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
        boolean isSystemDatabase = graphAPI.databaseLayout().getDatabaseName().startsWith( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        CypherConfiguration cypherConfig = CypherConfiguration.fromConfig( config );
        CypherPlannerConfiguration plannerConfig = cypherConfig.toCypherPlannerConfiguration( config, isSystemDatabase );
        CypherRuntimeConfiguration runtimeConfig = cypherConfig.toCypherRuntimeConfiguration();
        LogProvider logProvider = logService.getInternalLogProvider();
        CompilerFactory compilerFactory = makeCompilerFactory( queryService, monitors, logProvider, plannerConfig, runtimeConfig );
        deps.satisfyDependencies( compilerFactory );
        if ( isSystemDatabase )
        {
            CypherPlannerConfiguration innerPlannerConfig = cypherConfig.toCypherPlannerConfiguration( config, false );
            CommunityCompilerFactory innerCompilerFactory =
                    new CommunityCompilerFactory( queryService, monitors, logProvider, innerPlannerConfig, runtimeConfig );
            DatabaseManager databaseManager = resolver.resolveDependency( DatabaseManager.class );
            ExecutionEngine inner = new ExecutionEngine( queryService, logProvider, innerCompilerFactory );
            databaseManager.setInnerSystemExecutionEngine( new DatabaseManager.SystemDatabaseInnerEngine()
            {
                @Override
                public Result execute( String query, MapValue parameters, TransactionalContext context ) throws QueryExecutionKernelException
                {
                    return inner.executeQuery( query, parameters, context, false );
                }
            } );
            return new SystemExecutionEngine( queryService, logProvider, compilerFactory, innerCompilerFactory );
        }
        else if ( config.get( GraphDatabaseSettings.snapshot_query ) )
        {
            return new SnapshotExecutionEngine( queryService, config, logProvider, compilerFactory );
        }
        else
        {
            return new ExecutionEngine( queryService, logProvider, compilerFactory );
        }
    }
}
