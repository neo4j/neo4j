/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.neo4j.causalclustering.core.BoundedPriorityQueue.Result.E_COUNT_EXCEEDED;
import static org.neo4j.causalclustering.core.BoundedPriorityQueue.Result.E_SIZE_EXCEEDED;
import static org.neo4j.causalclustering.core.BoundedPriorityQueue.Result.OK;

/**
 * A bounded queue which is bounded both by the count of elements and by the total
 * size of all elements. The queue also has a minimum count which allows the queue
 * to always allow a minimum number of items, regardless of total size.
 *
 * @param <E> element type
 */
public class BoundedPriorityQueue<E>
{
    public static class Config
    {
        private final int minCount;
        private final int maxCount;
        private final long maxBytes;

        public Config( int maxCount, long maxBytes )
        {
            this( 1, maxCount, maxBytes );
        }

        public Config( int minCount, int maxCount, long maxBytes )
        {
            this.minCount = minCount;
            this.maxCount = maxCount;
            this.maxBytes = maxBytes;
        }
    }

    public interface Removable<E>
    {
        E get();

        boolean remove();

        default <T> Removable<T> map( Function<E,T> fn )
        {
            return new Removable<T>()
            {
                @Override
                public T get()
                {
                    return fn.apply( Removable.this.get() );
                }

                @Override
                public boolean remove()
                {
                    return Removable.this.remove();
                }
            };
        }
    }

    public enum Result
    {
        OK,
        E_COUNT_EXCEEDED,
        E_SIZE_EXCEEDED
    }

    private final Config config;
    private final Function<E,Long> sizeOf;

    private final BlockingQueue<StableElement> queue;
    private final AtomicLong seqGen = new AtomicLong();
    private final AtomicInteger count = new AtomicInteger();
    private final AtomicLong bytes = new AtomicLong();

    BoundedPriorityQueue( Config config, Function<E,Long> sizeOf, java.util.Comparator<E> comparator )
    {
        this.config = config;
        this.sizeOf = sizeOf;
        this.queue = new PriorityBlockingQueue<>( config.maxCount, new Comparator( comparator ) );
    }

    public int count()
    {
        return count.get();
    }

    public long bytes()
    {
        return bytes.get();
    }

    /**
     * Offers an element to the queue which gets accepted if neither the
     * element count nor the total byte limits are broken.
     *
     * @param element The element offered.
     * @return OK if successful, and a specific error code otherwise.
     */
    public Result offer( E element )
    {
        int updatedCount = count.incrementAndGet();
        if ( updatedCount > config.maxCount )
        {
            count.decrementAndGet();
            return E_COUNT_EXCEEDED;
        }

        long elementBytes = sizeOf.apply( element );
        long updatedBytes = bytes.addAndGet( elementBytes );

        if ( elementBytes != 0 && updatedCount > config.minCount )
        {
            if ( updatedBytes > config.maxBytes )
            {
                bytes.addAndGet( -elementBytes );
                count.decrementAndGet();
                return E_SIZE_EXCEEDED;
            }
        }

        if ( !queue.offer( new StableElement( element ) ) )
        {
            // this should not happen because we already have a reservation
            throw new IllegalStateException();
        }

        return OK;
    }

    /**
     * Helper for deducting the element and byte counts for a removed element.
     */
    private Optional<E> deduct( StableElement element )
    {
        if ( element == null )
        {
            return Optional.empty();
        }
        count.decrementAndGet();
        bytes.addAndGet( -sizeOf.apply( element.element ) );
        return Optional.of( element.element );
    }

    public Optional<E> poll()
    {
        return deduct( queue.poll() );
    }

    public Optional<E> poll( int timeout, TimeUnit unit ) throws InterruptedException
    {
        return deduct( queue.poll( timeout, unit ) );
    }

    Optional<Removable<E>> peek()
    {
        return Optional.ofNullable( queue.peek() );
    }

    class StableElement implements Removable<E>
    {
        private final long seqNo = seqGen.getAndIncrement();
        private final E element;

        StableElement( E element )
        {
            this.element = element;
        }

        @Override
        public E get()
        {
            return element;
        }

        @Override
        public boolean remove()
        {
            boolean removed = queue.remove( this );
            if ( removed )
            {
                deduct( this );
            }
            return removed;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            //noinspection unchecked
            StableElement that = (StableElement) o;
            return seqNo == that.seqNo;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( seqNo );
        }
    }

    class Comparator implements java.util.Comparator<BoundedPriorityQueue<E>.StableElement>
    {
        private final java.util.Comparator<E> comparator;

        Comparator( java.util.Comparator<E> comparator )
        {
            this.comparator = comparator;
        }

        @Override
        public int compare( BoundedPriorityQueue<E>.StableElement o1, BoundedPriorityQueue<E>.StableElement o2 )
        {
            int compare = comparator.compare( o1.element, o2.element );
            if ( compare != 0 )
            {
                return compare;
            }
            return Long.compare( o1.seqNo, o2.seqNo );
        }
    }
}
