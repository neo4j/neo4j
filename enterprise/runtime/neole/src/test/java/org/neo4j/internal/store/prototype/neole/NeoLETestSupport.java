/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.internal.store.prototype.neole;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.KernelAPIReadTestSupport;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assume.assumeThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

public class NeoLETestSupport implements KernelAPIReadTestSupport
{
    private File storeDir;

    @Override
    public void setup( File storeDir, Consumer<GraphDatabaseService> create ) throws IOException
    {
        GraphDatabaseService graphDb = null;
        try
        {
            GraphDatabaseBuilder builder = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
            builder.setConfig( dense_node_threshold, "1" );
            graphDb = builder.newGraphDatabase();
            create.accept( graphDb );
        }
        finally
        {
            if ( graphDb != null )
            {
                graphDb.shutdown();
            }
        }
        this.storeDir = storeDir;
    }

    @Override
    public void beforeEachTest()
    {
        assumeThat( "x86_64", equalTo( System.getProperty( "os.arch" ) ) );
    }

    @Override
    public Kernel kernelToTest()
    {
        try
        {
            return new NeoLEKernel( new ReadStore( storeDir ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to start kernel", e );
        }
    }

    @Override
    public void tearDown()
    {
        // runtime is closed by test
    }
}
