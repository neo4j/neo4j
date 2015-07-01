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

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.factory.EnterpriseFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

/**
 * Factory methods for building a Neo4j graph database, running in an enterprise HA cluster, for test purposes.
 */
public class EnterpriseHighAvailabilityTestGraphDatabase
{
    private EnterpriseHighAvailabilityTestGraphDatabase()
    {
    }

    /**
     * Start building a test HA graph database instance with the specified cluster member identifier.
     *
     * @param memberId the id to use for this cluster member
     * @return a builder for a test {@link HighAvailabilityGraphDatabase}
     */
    public static HighAvailabilityTestGraphDatabase.Builder withMemberId( int memberId )
    {
        return new Builder( memberId );
    }

    /**
     * Start building an impermanent HA graph database instance with the specified cluster member identifier.
     *
     * @param memberId the id to use for this cluster member
     * @return a builder for a test {@link HighAvailabilityGraphDatabase}
     */
    public static HighAvailabilityTestGraphDatabase.EphemeralBuilder ephemeralWithMemberId( int memberId )
    {
        return new EphemeralBuilder( memberId );
    }

    private static class Builder extends HighAvailabilityTestGraphDatabase.Builder
    {
        Builder( int memberId )
        {
            super( memberId );
        }

        @Override
        protected Builder self()
        {
            return this;
        }
    }

    private static class EphemeralBuilder extends HighAvailabilityTestGraphDatabase.EphemeralBuilder
    {
        EphemeralBuilder( int memberId )
        {
            super( memberId );
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

    static class GraphDatabaseImpl extends EnterpriseHighAvailabilityGraphDatabase.GraphDatabaseImpl implements HighAvailabilityTestGraphDatabase
    {
        GraphDatabaseImpl(
                EnterpriseFacadeFactory facadeFactory,
                File storeDir,
                Map<String,String> params,
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
