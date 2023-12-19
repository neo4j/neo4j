/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.graphdb.factory;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.EnterpriseGraphDatabase;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

import static org.neo4j.kernel.configuration.Settings.FALSE;

/**
 * Factory for Neo4j database instances with Enterprise Edition features.
 *
 * @see org.neo4j.graphdb.factory.GraphDatabaseFactory
 */
public class EnterpriseGraphDatabaseFactory extends GraphDatabaseFactory
{
    @Override
    protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator( final File storeDir,
                                                                          final GraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Map<String,String> config )
            {
                return newDatabase( Config.defaults( config ) );
            }

            @Override
            public GraphDatabaseService newDatabase( Config config )
            {
                config.augment( GraphDatabaseFacadeFactory.Configuration.ephemeral, FALSE );
                return new EnterpriseGraphDatabase( storeDir,
                        config,
                        state.databaseDependencies() );
            }
        };
    }

    @Override
    public String getEdition()
    {
        return Edition.enterprise.toString();
    }
}
