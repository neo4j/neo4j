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
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

public interface HighAvailabilityTestGraphDatabase extends HighAvailabilityGraphDatabase, TestGraphDatabase
{
    /**
     * Start building a test HA graph database instance with the specified cluster member identifier.
     *
     * @param memberId the id to use for this cluster member
     * @return a builder for a test {@link HighAvailabilityGraphDatabase}
     */
    static HighAvailabilityTestGraphDatabase.Builder withMemberId( int memberId )
    {
        return new Builder( memberId );
    }

    /**
     * Start building an impermanent HA graph database instance with the specified cluster member identifier.
     *
     * @param memberId the id to use for this cluster member
     * @return a builder for a test {@link HighAvailabilityGraphDatabase}
     */
    static HighAvailabilityTestGraphDatabase.EphemeralBuilder ephemeralWithMemberId( int memberId )
    {
        return new EphemeralBuilder( memberId );
    }

    class Builder extends HighAvailabilityTestGraphDatabaseBuilder<Builder,HighAvailabilityTestGraphDatabase>
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

        @Override
        protected HighAvailabilityTestGraphDatabase newInstance( File storeDir, Map<String,String> params,
                GraphDatabaseDependencies dependencies, GraphDatabaseFacadeFactory facadeFactory )
        {
            return new HighAvailabilityTestGraphDatabaseImpl( facadeFactory, storeDir, params, dependencies );
        }
    }

    class EphemeralBuilder extends HighAvailabilityTestGraphDatabaseBuilder<EphemeralBuilder,HighAvailabilityTestGraphDatabase>
    {
        public static final File PATH = new File( "target/test-data/impermanent-db" );

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

        @Override
        protected HighAvailabilityTestGraphDatabase newInstance( File storeDir, Map<String,String> params,
                GraphDatabaseDependencies dependencies, GraphDatabaseFacadeFactory facadeFactory )
        {
            return new HighAvailabilityTestGraphDatabaseImpl( facadeFactory, storeDir, params, dependencies );
        }
    }
}
