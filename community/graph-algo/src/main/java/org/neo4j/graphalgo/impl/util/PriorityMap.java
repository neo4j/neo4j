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
package org.neo4j.graphalgo.impl.util;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class PriorityMap<E, K, P>
{
    public interface Converter<T, S>
    {
        T convert( S source );
    }

    public static final class Entry<E, P>
    {
        private final E entity;
        private final P priority;

        private Entry( E entity, P priority )
        {
            this.entity = entity;
            this.priority = priority;
        }

        Entry( Node<E, P> node )
        {
            this( node.head.entity, node.head.priority );
        }

        public E getEntity()
        {
            return entity;
        }

        public P getPriority()
        {
            return priority;
        }
    }

    @SuppressWarnings( "rawtypes" )
    private static final Converter SELF_KEY = new Converter()
    {
        @Override
        public Object convert( Object source )
        {
            return source;
        }
    };
    @SuppressWarnings( "unchecked" )
    public static <K, P> PriorityMap<K, K, P> withSelfKey(
            Comparator<P> priority )
    {
        return new PriorityMap<K, K, P>( SELF_KEY, priority, true );
    }

    private static class NaturalPriority<P extends Comparable<P>> implements
            Comparator<P>
    {
        private final boolean reversed;

        NaturalPriority( boolean reversed )
        {
            this.reversed = reversed;
        }

        @Override
        public int compare( P o1, P o2 )
        {
            return reversed ? o2.compareTo( o1 ) : o1.compareTo( o2 );
        }
    }
    public static <E, K, P extends Comparable<P>> PriorityMap<E, K, P> withNaturalOrder(
            Converter<K, E> key )
    {
        return PriorityMap.<E, K, P>withNaturalOrder( key, false );
    }
    public static <E, K, P extends Comparable<P>> PriorityMap<E, K, P> withNaturalOrder(
            Converter<K, E> key, boolean reversed )
    {
        return withNaturalOrder( key, reversed, true );
    }
    public static <E, K, P extends Comparable<P>> PriorityMap<E, K, P> withNaturalOrder(
            Converter<K, E> key, boolean reversed, boolean onlyKeepBestPriorities )
    {
        Comparator<P> priority = new NaturalPriority<P>( reversed );
        return new PriorityMap<E, K, P>( key, priority, onlyKeepBestPriorities );
    }

    public static <K, P extends Comparable<P>> PriorityMap<K, K, P> withSelfKeyNaturalOrder()
    {
        return PriorityMap.<K, P>withSelfKeyNaturalOrder( false );
    }

    public static <K, P extends Comparable<P>> PriorityMap<K, K, P> withSelfKeyNaturalOrder(
            boolean reversed )
    {
        return PriorityMap.<K, P>withSelfKeyNaturalOrder( reversed, true );
    }

    @SuppressWarnings( "unchecked" )
    public static <K, P extends Comparable<P>> PriorityMap<K, K, P> withSelfKeyNaturalOrder(
            boolean reversed, boolean onlyKeepBestPriorities )
    {
        Comparator<P> priority = new NaturalPriority<P>( reversed );
        return new PriorityMap<K, K, P>( SELF_KEY, priority, onlyKeepBestPriorities );
    }

    private final Converter<K, E> keyFunction;
    private final Comparator<P> order;
    private final boolean onlyKeepBestPriorities;

    public PriorityMap( Converter<K, E> key, Comparator<P> priority, boolean onlyKeepBestPriorities )
    {
        this.keyFunction = key;
        this.order = priority;
        this.onlyKeepBestPriorities = onlyKeepBestPriorities;
    }

    /**
     * Add an entity to the priority map. If the key for the {@code entity}
     * was already found in the priority map and the priority is the same
     * the entity will be added. If the priority is lower the existing entities
     * for that key will be discarded.
     *
     * @param entity the entity to add.
     * @param priority the priority of the entity.
     * @return whether or not the entity (with its priority) was added to the
     * priority map. Will return {@code false} iff the key for the entity
     * already exist and its priority is better than the given
     * {@code priority}.
     */
    public boolean put( E entity, P priority )
    {
        K key = keyFunction.convert( entity );
        Node<E, P> node = map.get( key );
        boolean result = false;
        if ( node != null )
        {   // it already existed
            if ( onlyKeepBestPriorities )
            {
                if ( order.compare( priority, node.head.priority ) == 0 )
                {   // ...with same priority => add as a candidate first in chain
                    node.head = new Link<E,P>( entity, priority, node.head );
                    result = true;
                }
                else if ( order.compare( priority, node.head.priority ) < 0 )
                {   // ...with lower (better) priority => this new one replaces any existing
                    queue.remove( node );
                    putNew( entity, priority, key );
                    result = true;
                }
            }
            else
            {   // put in the appropriate place in the node linked list
                if ( order.compare( priority, node.head.priority ) < 0 )
                {   // ...first in chain and re-insert to queue
                    node.head = new Link<E,P>( entity, priority, node.head );
                    reinsert( node );
                    result = true;
                }
                else
                {   // we couldn't add it first in chain, go look for the appropriate place
                    Link<E,P> link = node.head, prev = link;
                    // skip the first one since we already compared head
                    link = link.next;
                    while ( link != null )
                    {
                        if ( order.compare( priority, link.priority ) <= 0 )
                        {   // here's our place, put it
                            // NODE ==> N ==> N ==> N
                            prev.next = new Link<E,P>( entity, priority, link );
                            result = true;
                            break;
                        }
                        prev = link;
                        link = link.next;
                    }
                    if ( !result )
                    {   // not added so append last in the chain
                        prev.next = new Link<E,P>( entity, priority, null );
                        result = true;
                    }
                }
            }
        }
        else
        {   // Didn't exist, just put
            putNew( entity, priority, key );
            result = true;
        }
        return result;
    }

    private void putNew( E entity, P priority, K key )
    {
        Node<E, P> node = new Node<E, P>( new Link<E,P>( entity, priority, null ) );
        map.put( key, node );
        queue.add( node );
    }

    private void reinsert( Node<E,P> node )
    {
        queue.remove( node );
        queue.add( node );
    }

    /**
     * Get the priority for the entity with the specified key.
     *
     * @param key the key.
     * @return the priority for the the entity with the specified key.
     */
    public P get( K key )
    {
        Node<E, P> node = map.get( key );
        return node != null ? node.head.priority : null;
    }

    /**
     * Remove and return the entry with the highest priority.
     *
     * @return the entry with the highest priority.
     */
    public Entry<E, P> pop()
    {
        Node<E, P> node = queue.peek();
        Entry<E, P> result = null;
        if ( node == null )
        {
            // Queue is empty
            return null;
        }
        else if ( node.head.next == null )
        {
            // There are no more entries attached to this key
            // Poll from queue and remove from map.
            node = queue.poll();
            map.remove( keyFunction.convert( node.head.entity ) );
            result = new Entry<E, P>( node );
        }
        else
        {
            result = new Entry<E, P>( node );
            node.head = node.head.next;
            if ( order.compare( result.priority, node.head.priority ) == 0 )
            {
                // Can leave at front of queue as priority is the same
                // Do nothing
            }
            else
            {
                // node needs to be reinserted into queue
                reinsert( node );
            }

        }
        return result;
    }

    public Entry<E, P> peek()
    {
        Node<E, P> node = queue.peek();
        if ( node == null )
        {
            return null;
        }
        return new Entry<E, P>( node );
    }

    // Naive implementation

    private final Map<K, Node<E, P>> map = new HashMap<K, Node<E, P>>();
    private final PriorityQueue<Node<E, P>> queue = new PriorityQueue<Node<E, P>>(
            11, new Comparator<Node<E, P>>()
            {
                @Override
                public int compare( Node<E, P> o1, Node<E, P> o2 )
                {
                    return order.compare( o1.head.priority, o2.head.priority );
                }
            } );

    private static class Node<E,P>
    {
        private Link<E,P> head;

        Node( Link<E,P> head )
        {
            this.head = head;
        }
    }

    private static class Link<E,P>
    {
        private final E entity;
        private final P priority;
        private Link<E,P> next;

        Link( E entity, P priority, Link<E,P> next )
        {
            this.entity = entity;
            this.priority = priority;
            this.next = next;
        }
    }
}
