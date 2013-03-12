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
package org.neo4j.kernel.impl.api.index;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Ignore;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.helpers.FutureAdapter;
import org.neo4j.kernel.api.index.SchemaIndexProvider;

@Ignore( "This is not a test" )
public class SchemaIndexTestHelper
{
    public static SchemaIndexProvider mockSchemaIndexProvider( String key )
    {
        SchemaIndexProvider mock = mock( SchemaIndexProvider.class );
        when( mock.getKey() ).thenReturn( key );
        return mock;
    }
    
    public static IndexProxy mockIndexProxy()
    {
        IndexProxy result = mock( IndexProxy.class );
        when( result.drop() ).thenReturn( FutureAdapter.VOID );
        when( result.close() ).thenReturn( FutureAdapter.VOID );
        return result;
    }
    
    public static <T> T awaitFuture( Future<T> future )
    {
        try
        {
            return future.get( 10, SECONDS );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new RuntimeException( e );
        }
        catch ( ExecutionException e )
        {
            throw new RuntimeException( e );
        }
        catch ( TimeoutException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    public static void awaitLatch( CountDownLatch latch )
    {
        try
        {
            latch.await( 10, SECONDS );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new RuntimeException( e );
        }
    }

    public static void awaitIndexState( GraphDatabaseService db, IndexDefinition index, IndexState state )
    {
        long timeot = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 10 );
        while( db.schema().getIndexState( index )  != state )
        {
            Thread.yield();
            if ( System.currentTimeMillis() > timeot )
                fail( "Expected index to come online within a reasonable time." );
        }
    }
}
