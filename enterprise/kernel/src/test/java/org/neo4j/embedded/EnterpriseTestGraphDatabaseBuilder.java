/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.embedded;

import java.io.File;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.enterprise.EnterpriseFacadeFactory;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.logging.AbstractLogService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.LogProvider;

abstract class EnterpriseTestGraphDatabaseBuilder<
        BUILDER extends EnterpriseTestGraphDatabaseBuilder<BUILDER,GRAPHDB>, GRAPHDB extends EnterpriseTestGraphDatabase>
        extends TestGraphDatabaseBuilder<BUILDER,EnterpriseTestGraphDatabase>
{
    @Override
    protected GraphDatabaseFacadeFactory createFacadeFactory()
    {
        // TODO: replace overriding with dependency injection
        return new EnterpriseFacadeFactory()
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Map<String,String> params,
                    Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade )
            {
                return new PlatformModule( storeDir, params, dependencies, graphDatabaseFacade )
                {
                    @Override
                    protected FileSystemAbstraction createFileSystemAbstraction()
                    {
                        return (fs != null) ? fs : super.createFileSystemAbstraction();
                    }

                    @Override
                    protected LogService createLogService( final LogProvider userLogProvider )
                    {
                        if ( internalLogProvider == null )
                        {
                            return super.createLogService( userLogProvider );
                        }

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

            @Override
            protected EditionModule createEdition( PlatformModule platformModule )
            {
                return new EnterpriseEditionModule( platformModule )
                {
                    @Override
                    protected IdGeneratorFactory createIdGeneratorFactory( FileSystemAbstraction fs )
                    {
                        return (idFactory != null) ? idFactory : super.createIdGeneratorFactory( fs );
                    }
                };
            }
        };
    }
}
