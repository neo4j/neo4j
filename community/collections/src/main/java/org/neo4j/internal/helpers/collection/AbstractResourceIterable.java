/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.helpers.collection;

import org.eclipse.collections.impl.set.strategy.mutable.MutableHashingStrategySetFactoryImpl;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

import static org.eclipse.collections.impl.block.factory.HashingStrategies.identityStrategy;

public abstract class AbstractResourceIterable<T> implements ResourceIterable<T>
{

    private final Set<ResourceIterator<T>> resourceIterators = MutableHashingStrategySetFactoryImpl.INSTANCE
            .withInitialCapacity( identityStrategy(), 2 );

    private boolean closed;

    protected abstract ResourceIterator<T> newIterator();

    @Override
    public final ResourceIterator<T> iterator()
    {
        if ( closed )
        {
            throw new ResourceIteratorCloseFailedException( ResourceIterable.class.getSimpleName() + " has already been closed" );
        }

        final var resourceIterator = Objects.requireNonNull( newIterator() );
        register( resourceIterator );
        return new ResourceIterator<>()
        {
            @Override
            public boolean hasNext()
            {
                return resourceIterator.hasNext();
            }

            @Override
            public T next()
            {
                return resourceIterator.next();
            }

            @Override
            public Stream<T> stream()
            {
                return resourceIterator.stream();
            }

            @Override
            public <R> ResourceIterator<R> map( Function<T,R> map )
            {
                return resourceIterator.map( map );
            }

            @Override
            public void close()
            {
                try
                {
                    resourceIterator.close();
                }
                finally
                {
                    unregister( resourceIterator );
                }
            }
        };
    }

    @Override
    public final void close()
    {
        if ( !closed )
        {
            try
            {
                closeAll();
            }
            finally
            {
                closed = true;
                onClosed();
            }
        }
    }

    /**
     * Callback method that allows subclasses to perform their own specific closing logic
     */
    protected void onClosed()
    {
    }

    private void register( ResourceIterator<T> closeable )
    {
        resourceIterators.add( closeable );
    }

    private void unregister( ResourceIterator<T> closeable )
    {
        resourceIterators.remove( closeable );
    }

    private void closeAll()
    {
        try
        {
            ResourceIteratorCloseFailedException closeThrowable = null;
            for ( final var resourceIterator : resourceIterators )
            {
                try
                {
                    resourceIterator.close();
                }
                catch ( Exception e )
                {
                    if ( closeThrowable == null )
                    {
                        closeThrowable = new ResourceIteratorCloseFailedException( "Exception closing a resource iterator.", e );
                    }
                    else
                    {
                        closeThrowable.addSuppressed( e );
                    }
                }
            }
            if ( closeThrowable != null )
            {
                throw closeThrowable;
            }
        }
        finally
        {
            resourceIterators.clear();
        }
    }
}
