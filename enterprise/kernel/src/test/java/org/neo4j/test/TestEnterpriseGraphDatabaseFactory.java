/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.enterprise.EnterpriseFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.AbstractLogService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.LogProvider;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

public class TestEnterpriseGraphDatabaseFactory extends TestGraphDatabaseFactory
{
    @Override
    protected GraphDatabaseBuilder.DatabaseCreator createImpermanentDatabaseCreator( final File storeDir,
            final TestGraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            @SuppressWarnings( "deprecation" )
            public GraphDatabaseService newDatabase( Map<String,String> config )
            {
                return new EnterpriseFacadeFactory()
                {
                    @Override
                    protected PlatformModule createPlatform( File storeDir, Map<String,String> params,
                            Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                            OperationalMode operationalMode)
                    {
                        return new ImpermanentGraphDatabase.ImpermanentPlatformModule( storeDir, params, dependencies,
                                graphDatabaseFacade )
                        {
                            @Override
                            protected FileSystemAbstraction createFileSystemAbstraction()
                            {
                                FileSystemAbstraction fs = state.getFileSystem();
                                if ( fs != null )
                                {
                                    return fs;
                                }
                                else
                                {
                                    return super.createFileSystemAbstraction();
                                }
                            }

                            @Override
                            protected LogService createLogService( LogProvider logProvider )
                            {
                                final LogProvider internalLogProvider = state.getInternalLogProvider();
                                if ( internalLogProvider == null )
                                {
                                    return super.createLogService( logProvider );
                                }

                                final LogProvider userLogProvider = state.databaseDependencies().userLogProvider();
                                return new AbstractLogService()
                                {
                                    @Override
                                    public LogProvider getUserLogProvider()
                                    {
                                        return userLogProvider;
                                    }

                                    @Override
                                    public LogProvider getInternalLogProvider()
                                    {
                                        return internalLogProvider;
                                    }
                                };
                            }

                        };
                    }
                }.newFacade( storeDir, config,
                        GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
            }
        };
    }
}
