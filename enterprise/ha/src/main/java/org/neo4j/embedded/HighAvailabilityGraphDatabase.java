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

import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

/**
 * A running Neo4j Enterprise high availability graph database.
 */
public interface HighAvailabilityGraphDatabase extends EnterpriseGraphDatabase
{
    /**
     * Roles the high availability graph database may currently be in
     */
    enum Role
    {
        /**
         * The master of the HA cluster
         */
        MASTER,
        /**
         * A slave within the HA cluster
         */
        SLAVE,
        /**
         * Currently unknown - the database is either trying to join the cluster or is in the middle of changing roles
         */
        UNKNOWN
    }

    /**
     * Start building a HA graph database instance with the specified cluster member identifier.
     *
     * @param memberId the id to use for this cluster member
     * @return a builder for a {@link HighAvailabilityGraphDatabase}
     */
    static HighAvailabilityGraphDatabase.Builder withMemberId( int memberId )
    {
        return new Builder( memberId );
    }

    /**
     * A builder for a {@link HighAvailabilityGraphDatabase}
     */
    class Builder extends HighAvailabilityGraphDatabaseBuilder<Builder,HighAvailabilityGraphDatabase>
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
        protected HighAvailabilityGraphDatabase newInstance( File storeDir, Map<String,String> params,
                GraphDatabaseDependencies dependencies, GraphDatabaseFacadeFactory facadeFactory )
        {
            return new HighAvailabilityGraphDatabaseImpl( facadeFactory, storeDir, params, dependencies );
        }
    }

    /**
     * @return true if this database is the master of the cluster, and false otherwise
     */
    boolean isMaster();

    /**
     * Obtain the current role of the this database in the HA cluster.
     *
     * @return the current role of this database in the HA cluster
     */
    Role haClusterRole();
}
