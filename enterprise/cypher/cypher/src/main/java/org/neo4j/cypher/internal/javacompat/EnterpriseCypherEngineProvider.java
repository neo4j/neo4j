/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.javacompat;

import org.neo4j.cypher.internal.CommunityCompatibilityFactory;
import org.neo4j.cypher.internal.EnterpriseCompatibilityFactory;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

@Service.Implementation( QueryEngineProvider.class )
public class EnterpriseCypherEngineProvider extends QueryEngineProvider
{
    public EnterpriseCypherEngineProvider()
    {
        super( "enterprise-cypher" );
    }

    @Override
    protected int enginePriority()
    {
        return 1; // Lower means better. The enterprise version will have a lower number
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
        LogProvider logProvider = logService.getInternalLogProvider();
        CommunityCompatibilityFactory inner =
                new CommunityCompatibilityFactory( queryService, monitors, logProvider );

        EnterpriseCompatibilityFactory compatibilityFactory =
                new EnterpriseCompatibilityFactory( inner, queryService, monitors, logProvider );
        deps.satisfyDependency( compatibilityFactory );
        return createEngine( queryService, config, logProvider, compatibilityFactory );
    }

    private QueryExecutionEngine createEngine( GraphDatabaseCypherService queryService, Config config,
            LogProvider logProvider, EnterpriseCompatibilityFactory compatibilityFactory )
    {
        return config.get( GraphDatabaseSettings.snapshot_query ) ?
               snapshotEngine( queryService, config, logProvider, compatibilityFactory ) :
               standardEngine( queryService, logProvider, compatibilityFactory );
    }

    private SnapshotExecutionEngine snapshotEngine( GraphDatabaseCypherService queryService, Config config,
            LogProvider logProvider, EnterpriseCompatibilityFactory compatibilityFactory )
    {
        return new SnapshotExecutionEngine( queryService, config, logProvider, compatibilityFactory );
    }

    private ExecutionEngine standardEngine( GraphDatabaseCypherService queryService, LogProvider logProvider,
            EnterpriseCompatibilityFactory compatibilityFactory )
    {
        return new ExecutionEngine( queryService, logProvider, compatibilityFactory );
    }
}
