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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

/**
 * A running Neo4j Enterprise graph database. Provides methods for controlling the database instance (e.g. shutdown,
 * adding event handlers, etc), and also implements {@link GraphDatabaseService} to provides all the methods
 * for working with the graph itself.
 */
public interface EnterpriseGraphDatabase extends GraphDatabase
{
    /**
     * Start building a Graph Database.
     *
     * @return a builder for a {@link GraphDatabase}
     */
    static EnterpriseGraphDatabase.Builder build()
    {
        return new Builder();
    }

    /**
     * Start a graph database by opening the specified filesystem directory containing the store.
     *
     * @param storeDir The filesystem location for the store, which will be created if necessary
     * @return The running database
     */
    static EnterpriseGraphDatabase open( File storeDir )
    {
        return build().open( storeDir );
    }

    /**
     * A builder for a {@link GraphDatabase}
     */
    class Builder extends EnterpriseGraphDatabaseBuilder<Builder,EnterpriseGraphDatabase>
    {
        @Override
        protected Builder self()
        {
            return this;
        }

        @Override
        protected EnterpriseGraphDatabase newInstance( File storeDir, Map<String,String> params,
                GraphDatabaseDependencies dependencies, GraphDatabaseFacadeFactory facadeFactory )
        {
            return new EnterpriseGraphDatabaseImpl( facadeFactory, storeDir, params, dependencies );
        }
    }
}
