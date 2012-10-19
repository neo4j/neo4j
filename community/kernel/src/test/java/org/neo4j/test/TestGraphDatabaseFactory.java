/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.util.Map;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * Test factory for graph databases
 */
public class TestGraphDatabaseFactory
    extends GraphDatabaseFactory
{
    public GraphDatabaseService newImpermanentDatabase()
    {
        return newImpermanentDatabaseBuilder().newGraphDatabase();
    }
    
    public GraphDatabaseBuilder newImpermanentDatabaseBuilder()
    {
        return new GraphDatabaseBuilder(new GraphDatabaseBuilder.DatabaseCreator()
        {
            public GraphDatabaseService newDatabase(Map<String, String> config)
            {
                config.put( "ephemeral", "true" );
                return new ImpermanentGraphDatabase(config, indexProviders, kernelExtensions, cacheProviders,
                        txInterceptorProviders);
            }
        });
    }
}
