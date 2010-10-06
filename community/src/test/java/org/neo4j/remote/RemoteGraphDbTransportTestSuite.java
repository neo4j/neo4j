/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.remote;

import java.io.File;
import java.util.concurrent.Callable;

import org.junit.runner.RunWith;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.remote.test.BasicApiTest;
import org.neo4j.remote.test.IndexTest;
import org.neo4j.remote.test.TheMatrixTest;
import org.neo4j.remote.transports.LocalGraphDatabase;
import org.neo4j.testsupport.PhaseRunner;
import org.neo4j.testsupport.PhaseRunner.Phase;

@RunWith( PhaseRunner.class )
public abstract class RemoteGraphDbTransportTestSuite
{
    /**
     * Create and register a remote graph database server, returning its
     * resource uri.
     *
     * @param graphDb the {@link GraphDatabaseService} to use as server backend.
     * @param index
     * @return the resource uri of the newly created server.
     * @throws Exception if anything fails.
     */
    protected abstract String createServer( GraphDatabaseService graphDb,
            IndexService index ) throws Exception;

    // <PHASES>

    public @Phase
    AbstractTestBase testNothing() throws Exception
    {
        return new TestNothing();
    }

    public @Phase
    AbstractTestBase testBasicAPI() throws Exception
    {
        return new BasicApiTest( FACTORY );
    }

    public @Phase
    AbstractTestBase testIndexAPI() throws Exception
    {
        return new IndexTest( FACTORY );
    }

    public @Phase
    AbstractTestBase testTheMatrix() throws Exception
    {
        return new TheMatrixTest( FACTORY );
    }

    // </PHASES>


    // ===== PRIVATE IMPLEMENTATION =====

    protected BasicGraphDatabaseServer basicServer(
            GraphDatabaseService graphDb, IndexService index )
    {
        BasicGraphDatabaseServer server = new LocalGraphDatabase( graphDb );
        server.registerIndexService( "index", index );
        return server;
    }

    private final Callable<RemoteGraphDatabase> FACTORY = new Callable<RemoteGraphDatabase>()
    {
        public RemoteGraphDatabase call() throws Exception
        {
            return graphDb();
        }
    };

    private Callable<RemoteGraphDatabase> factory = null;

    private RemoteGraphDatabase graphDb() throws Exception
    {
        if ( factory == null )
        {
            factory = prepareServer( createCleanGraphDatabase( "target/neodb" ) );
        }
        return factory.call();
    }

    private static EmbeddedGraphDatabase graphDb;

    private static GraphDatabaseService createCleanGraphDatabase(
            String storeDir )
    {
        if ( graphDb != null )
        {
            graphDb.shutdown();
            graphDb = null;
        }
        deleteFileOrDirectory( new File( storeDir ) );
        return graphDb = new EmbeddedGraphDatabase( storeDir );
    }

    private static void deleteFileOrDirectory( File file )
    {
        if ( !file.exists() )
        {
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
        }
        else
        {
            file.delete();
        }
    }

    private Callable<RemoteGraphDatabase> prepareServer(
            GraphDatabaseService graphDb ) throws Exception
    {
        final IndexService index = new LuceneIndexService( graphDb );
        graphDb.registerKernelEventHandler( new KernelEventHandler()
        {
            public void beforeShutdown()
            {
                index.shutdown();
            }

            public Object getResource()
            {
                return index;
            }

            public void kernelPanic( ErrorState error )
            {
            }

            public ExecutionOrder orderComparedTo( KernelEventHandler other )
            {
                return ExecutionOrder.DOESNT_MATTER;
            }
        } );
        return prepareServer( graphDb, index );
    }

    protected Callable<RemoteGraphDatabase> prepareServer(
            GraphDatabaseService graphDb, IndexService index ) throws Exception
    {
        final String resourceUri = createServer( graphDb, index );
        return new Callable<RemoteGraphDatabase>()
        {
            public RemoteGraphDatabase call() throws Exception
            {
                return new RemoteGraphDatabase( resourceUri );
            }
        };
    }
}
