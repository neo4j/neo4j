/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;

public class SchemaIndexTestHelper
{
    private SchemaIndexTestHelper()
    {
    }

    public static KernelExtensionFactory<SingleInstanceIndexProviderFactoryDependencies> singleInstanceIndexProviderFactory(
            String key, final IndexProvider provider )
    {
        return new SingleInstanceIndexProviderFactory( key, provider );
    }

    public interface SingleInstanceIndexProviderFactoryDependencies
    {
        Config config();
    }

    private static class SingleInstanceIndexProviderFactory
        extends KernelExtensionFactory<SingleInstanceIndexProviderFactoryDependencies>
    {
        private final IndexProvider provider;

        private SingleInstanceIndexProviderFactory( String key, IndexProvider provider )
        {
            super( key );
            this.provider = provider;
        }

        @Override
        public Lifecycle newInstance( KernelContext context,
                SingleInstanceIndexProviderFactoryDependencies dependencies )
        {
            return provider;
        }
    }

    public static IndexProxy mockIndexProxy() throws IOException
    {
        return mock( IndexProxy.class );
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

    public static void awaitIndexOnline( SchemaRead schemaRead, IndexReference index )
            throws IndexNotFoundKernelException
    {
        long start = System.currentTimeMillis();
        while ( true )
        {
            if ( schemaRead.indexGetState( index ) == InternalIndexState.ONLINE )
            {
                break;
            }

            if ( start + 1000 * 10 < System.currentTimeMillis() )
            {
                throw new RuntimeException( "Index didn't come online within a reasonable time." );
            }
        }
    }
}
