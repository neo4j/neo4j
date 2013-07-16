/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.enterprise;

import java.util.Map;

import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.CommunityDatabase;
import org.neo4j.server.database.GraphDatabaseFactory;

public class EnterpriseDatabase extends CommunityDatabase
{
    enum DatabaseMode implements GraphDatabaseFactory
    {
        SINGLE
                {
                    @Override
                    public GraphDatabaseAPI createDatabase( String databaseStoreDirectory,
                                                            Map<String, String> databaseProperties )
                    {
                        return (GraphDatabaseAPI) new org.neo4j.graphdb.factory.GraphDatabaseFactory().
                                newEmbeddedDatabaseBuilder( databaseStoreDirectory).
                                setConfig( databaseProperties ).newGraphDatabase();
                    }
                },
        HA
                {
                    @Override
                    public GraphDatabaseAPI createDatabase( String databaseStoreDirectory,
                                                            Map<String, String> databaseProperties )
                    {
//                        List<IndexProvider> indexProviders = Iterables.toList( Service.load( IndexProvider.class ) );
//                        List<KernelExtensionFactory<?>> kernelExtensions = Iterables.toList( Iterables
//                                .<KernelExtensionFactory<?>, KernelExtensionFactory>cast( Service.load(
//                                        KernelExtensionFactory
//                                .class ) ) );
//                        List<CacheProvider> cacheProviders = Iterables.toList( Service.load( CacheProvider.class ) );
//                        List<TransactionInterceptorProvider> txInterceptorProviders =
//                                Iterables.toList( Service.load( TransactionInterceptorProvider.class ) );
//                        List<SchemaIndexProvider> schemaIndexProviders =
//                                Iterables.toList( Service.load( SchemaIndexProvider.class ) );
//                        return new HighlyAvailableGraphDatabase( databaseStoreDirectory, databaseProperties,
//                                indexProviders, kernelExtensions, cacheProviders, txInterceptorProviders );
                        return (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory().
                                newHighlyAvailableDatabaseBuilder( databaseStoreDirectory ).
                                setConfig( databaseProperties ).newGraphDatabase();
                    }
                };

        @Override
        public abstract GraphDatabaseAPI createDatabase( String databaseStoreDirectory,
                                                         Map<String, String> databaseProperties );
    }

    public EnterpriseDatabase( Configurator configurator )
    {
        super( configurator );
    }

    @Override
    @SuppressWarnings("deprecation")
    public void start() throws Throwable
    {
        try
        {
            GraphDatabaseFactory factory = DatabaseMode.valueOf( serverConfiguration.getString(
                    Configurator.DB_MODE_KEY, DatabaseMode.SINGLE.name() ).toUpperCase() );

            this.graph = (AbstractGraphDatabase) factory.createDatabase(
                    serverConfiguration.getString( Configurator.DATABASE_LOCATION_PROPERTY_KEY,
                            Configurator.DEFAULT_DATABASE_LOCATION_PROPERTY_KEY ),
                    getDbTuningPropertiesWithServerDefaults() );

            log.info( "Successfully started database" );
        }
        catch ( Exception e )
        {
            log.error( "Failed to start database.", e );
            throw e;
        }
    }
}
