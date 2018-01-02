/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Ignore;
import org.neo4j.helpers.FutureAdapter;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.*;

@Ignore( "This is not a test" )
public class SchemaIndexTestHelper
{
    public static KernelExtensionFactory<SingleInstanceSchemaIndexProviderFactoryDependencies> singleInstanceSchemaIndexProviderFactory(
            String key, final SchemaIndexProvider provider )
    {
        return new SingleInstanceSchemaIndexProviderFactory( key, provider );
    }
    
    public interface SingleInstanceSchemaIndexProviderFactoryDependencies
    {
        Config config();
    }
    
    private static class SingleInstanceSchemaIndexProviderFactory
        extends KernelExtensionFactory<SingleInstanceSchemaIndexProviderFactoryDependencies>
    {
        private final SchemaIndexProvider provider;

        private SingleInstanceSchemaIndexProviderFactory( String key, SchemaIndexProvider provider )
        {
            super( key );
            this.provider = provider;
        }

        @Override
        public Lifecycle newKernelExtension( SingleInstanceSchemaIndexProviderFactoryDependencies dependencies )
                throws Throwable
        {
            return provider;
        }
    }
    
    public static IndexProxy mockIndexProxy() throws IOException
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
        catch ( ExecutionException | TimeoutException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    public static boolean awaitLatch( CountDownLatch latch )
    {
        try
        {
            return latch.await( 10, SECONDS );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
            throw new RuntimeException( e );
        }
    }
    
    public static void awaitIndexOnline( ReadOperations readOperations, IndexDescriptor indexRule )
            throws IndexNotFoundKernelException
    {
        long start = System.currentTimeMillis();
        while(true)
        {
            if ( readOperations.indexGetState( indexRule ) == InternalIndexState.ONLINE )
           {
               break;
           }

           if(start + 1000 * 10 < System.currentTimeMillis())
           {
               throw new RuntimeException( "Index didn't come online within a reasonable time." );
           }
        }
    }
}
