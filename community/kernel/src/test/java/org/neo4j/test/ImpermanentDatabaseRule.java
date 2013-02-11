/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.test;

import java.io.IOException;
import org.junit.rules.ExternalResource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;

/**
 * JUnit @Rule for configuring, creating and managing an ImpermanentGraphDatabase instance.
 */
public class ImpermanentDatabaseRule
    extends ExternalResource
{
    GraphDatabaseAPI database;

    @Override
    protected void before()
        throws Throwable
    {
        create();
    }

    @Override
    protected void after()
    {
        shutdown();
    }

    public void create()
        throws IOException
    {
        TestGraphDatabaseFactory databaseFactory = new TestGraphDatabaseFactory();
        configure( databaseFactory );
        GraphDatabaseBuilder builder = databaseFactory.newImpermanentDatabaseBuilder();
        configure(builder);
        database = (GraphDatabaseAPI) builder.newGraphDatabase();
    }

    protected void configure( GraphDatabaseFactory databaseFactory )
    {
        // Override to configure the database factory
    }

    protected void configure( GraphDatabaseBuilder builder )
    {
        // Override to configure the database builder
    }
    
    public GraphDatabaseService getGraphDatabaseService()
    {
        return database;
    }

    public GraphDatabaseAPI getGraphDatabaseAPI()
    {
        return database;
    }

    public void shutdown()
    {
        if (database != null)
            database.shutdown();
    }
}
