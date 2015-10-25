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
import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

/**
 * A running Neo4j graph database, with additional methods exposed for test purposes.
 */
public interface TestGraphDatabase extends GraphDatabase
{
    /**
     * Start building a test Graph Database.
     *
     * @return a builder for a test {@link TestGraphDatabase}
     */
    static TestGraphDatabase.Builder build()
    {
        return new Builder();
    }

    /**
     * Start a graph database by opening the specified filesystem directory containing the store.
     *
     * @param storeDir The filesystem location for the store, which will be created if necessary
     * @return The running database
     */
    static TestGraphDatabase open( File storeDir )
    {
        return build().open( storeDir );
    }

    /**
     * Start building a test Graph Database, using an ephemeral filesystem.
     *
     * @return a builder for an ephemeral {@link TestGraphDatabase}
     */
    static TestGraphDatabase.EphemeralBuilder buildEphemeral()
    {
        return new EphemeralBuilder();
    }

    /**
     * Start a graph database using an ephemeral filesystem.
     *
     * @return a builder for an ephemeral {@link TestGraphDatabase}
     */
    static TestGraphDatabase openEphemeral()
    {
        return buildEphemeral().open();
    }

    class Builder extends TestGraphDatabaseBuilder<Builder,TestGraphDatabase>
    {
        @Override
        protected Builder self()
        {
            return this;
        }

        @Override
        protected TestGraphDatabase newInstance( File storeDir, Map<String,String> params,
                GraphDatabaseDependencies dependencies, GraphDatabaseFacadeFactory facadeFactory )
        {
            return new TestGraphDatabaseImpl( facadeFactory, storeDir, params, dependencies );
        }
    }

    class EphemeralBuilder extends TestGraphDatabaseBuilder<EphemeralBuilder,TestGraphDatabase>
    {
        private static final File PATH = new File( "target/test-data/impermanent-db" );

        EphemeralBuilder()
        {
            withParam( "ephemeral", "true" );
            withFileSystem( new EphemeralFileSystemAbstraction() );
        }

        public TestGraphDatabase open()
        {
            return open( PATH );
        }

        @Override
        protected EphemeralBuilder self()
        {
            return this;
        }

        @Override
        protected TestGraphDatabase newInstance( File storeDir, Map<String,String> params,
                GraphDatabaseDependencies dependencies, GraphDatabaseFacadeFactory facadeFactory )
        {
            return new TestGraphDatabaseImpl( facadeFactory, storeDir, params, dependencies );
        }
    }

    FileSystemAbstraction fileSystem();

    File storeDir();

    DependencyResolver getDependencyResolver();

    /**
     * @deprecated Method included for transitional purpose only - prefer not using this method
     */
    @Deprecated
    GraphDatabaseAPI getGraphDatabaseAPI();
}
