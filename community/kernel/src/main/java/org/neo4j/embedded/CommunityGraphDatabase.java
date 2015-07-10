/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.embedded;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.LogProvider;

/**
 * Factory methods for building a Neo4j graph database.
 */
// TODO: when building with Java 8, the content of this class can be moved into GraphDatabase
public class CommunityGraphDatabase
{
    private CommunityGraphDatabase()
    {
    }

    /**
     * Start building a Graph Database.
     *
     * @return a builder for a {@link CommunityGraphDatabase}
     */
    public static GraphDatabase.Builder build()
    {
        return new Builder();
    }

    /**
     * Start a graph database by opening the specified filesystem directory containing the store.
     *
     * @param storeDir The filesystem location for the store, which will be created if necessary
     * @return The running database
     */
    public static GraphDatabase open( File storeDir )
    {
        return build().open( storeDir );
    }

    private static class Builder extends GraphDatabase.Builder
    {
        @Override
        protected Builder self()
        {
            return this;
        }

        @Override
        protected GraphDatabase newInstance(
                File storeDir,
                LogProvider logProvider,
                Map<String,String> params,
                List<KernelExtensionFactory<?>> kernelExtensions )
        {
            GraphDatabaseFacadeFactory.Dependencies dependencies = GraphDatabaseDependencies.newDependencies()
                    .userLogProvider( logProvider )
                    .kernelExtensions( kernelExtensions )
                    .settingsClasses( GraphDatabaseSettings.class );
            CommunityFacadeFactory facadeFactory = new CommunityFacadeFactory();
            return new GraphDatabaseImpl( facadeFactory, storeDir, params, dependencies );
        }
    }

    static class GraphDatabaseImpl extends GraphDatabaseFacade implements GraphDatabase
    {
        GraphDatabaseImpl(
                CommunityFacadeFactory facadeFactory,
                File storeDir,
                Map<String,String> params,
                GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            facadeFactory.newFacade( storeDir, params, dependencies, this );
        }
    }
}
