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

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

/**
 * Factory methods for building a Neo4j graph database, for test purposes.
 */
// TODO: when building with Java 8, the content of this class can be moved into GraphDatabase
public class CommunityTestGraphDatabase
{
    private CommunityTestGraphDatabase()
    {
    }

    /**
     * Start building a test Graph Database.
     *
     * @return a builder for a test {@link TestGraphDatabase}
     */
    public static TestGraphDatabase.Builder build()
    {
        return new Builder();
    }

    /**
     * Start a graph database by opening the specified filesystem directory containing the store.
     *
     * @param storeDir The filesystem location for the store, which will be created if necessary
     * @return The running database
     */
    public static TestGraphDatabase open( File storeDir )
    {
        return build().open( storeDir );
    }

    /**
     * Start building a test Graph Database, using an ephemeral filesystem.
     *
     * @return a builder for an ephemeral {@link TestGraphDatabase}
     */
    public static TestGraphDatabase.EphemeralBuilder buildEphemeral()
    {
        return new EphemeralBuilder();
    }

    /**
     * Start a graph database by opening the specified filesystem directory containing the store.
     */
    public static TestGraphDatabase openEphemeral()
    {
        return buildEphemeral().open();
    }

    private static class Builder extends TestGraphDatabase.Builder
    {
        @Override
        protected Builder self()
        {
            return this;
        }
    }

    private static class EphemeralBuilder extends TestGraphDatabase.EphemeralBuilder
    {
        private static final File PATH = new File( "target/test-data/impermanent-db" );

        EphemeralBuilder()
        {
            params.put( "ephemeral", "true" );
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
    }

    static class GraphDatabaseImpl extends CommunityGraphDatabase.GraphDatabaseImpl implements TestGraphDatabase
    {
        GraphDatabaseImpl(
                CommunityFacadeFactory facadeFactory,
                File storeDir, Map<String,String> params,
                GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            super( facadeFactory, storeDir, params, dependencies );
        }

        @Override
        public FileSystemAbstraction fileSystem()
        {
            return fileSystem;
        }

        @Override
        public File storeDir()
        {
            return storeDir;
        }

        @Override
        public GraphDatabaseAPI getGraphDatabaseAPI()
        {
            return this;
        }
    }
}
