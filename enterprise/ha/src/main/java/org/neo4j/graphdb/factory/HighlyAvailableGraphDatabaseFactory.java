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
package org.neo4j.graphdb.factory;

import java.io.File;
import java.util.Map;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

import static java.util.Arrays.asList;

/**
 * Factory for Neo4j database instances with Enterprise Edition and High-Availability features.
 *
 * @see org.neo4j.graphdb.factory.GraphDatabaseFactory
 */
public class HighlyAvailableGraphDatabaseFactory extends GraphDatabaseFactory
{
    public HighlyAvailableGraphDatabaseFactory()
    {
        super( highlyAvailableFactoryState() );
    }

    private static GraphDatabaseFactoryState highlyAvailableFactoryState()
    {
        GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
        state.addSettingsClasses( asList( ClusterSettings.class, HaSettings.class ) );
        return state;
    }

    @Override
    protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator(
            final File storeDir, final GraphDatabaseFactoryState state )
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
                config.augment( GraphDatabaseFacadeFactory.Configuration.ephemeral, "false" );
                return new HighlyAvailableGraphDatabase( storeDir, config, state.databaseDependencies() );
            }
        };
    }

    @Override
    public String getEdition()
    {
        return Edition.enterprise.toString();
    }
}
