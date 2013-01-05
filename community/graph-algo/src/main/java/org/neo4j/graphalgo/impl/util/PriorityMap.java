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
package org.neo4j.graphalgo.impl.util;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

class PriorityMap<E, K, P>
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
            this( node.head.entity, node.priority );
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

    @SuppressWarnings( "unchecked" )
    private static final Converter SELF_KEY = new Converter()
    {
        public Object convert( Object source )
        {
            return source;
        }
    };
    @SuppressWarnings( "unchecked" )
    public static <K, P> PriorityMap<K, K, P> withSelfKey(
            Comparator<P> priority )
    {
        return new PriorityMap<K, K, P>( SELF_KEY, priority );
    }

    private static class NaturalPriority<P extends Comparable<P>> implements
            Comparator<P>
    {
        private final boolean reversed;

        NaturalPriority( boolean reversed )
        {
            this.reversed = reversed;
        }

        public int compare( P o1, P o2 )
        {
            if ( reversed )
            {
                return o2.compareTo( o1 );
            }
            else
            {
                return o1.compareTo( o2 );
            }
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
        Comparator<P> priority = new NaturalPriority<P>( reversed );
        return new PriorityMap<E, K, P>( key, priority );
    }

    public static <K, P extends Comparable<P>> PriorityMap<K, K, P> withSelfKeyNaturalOrder()
    {
        return PriorityMap.<K, P>withSelfKeyNaturalOrder( false );
    }
    @SuppressWarnings( "unchecked" )
    public static <K, P extends Comparable<P>> PriorityMap<K, K, P> withSelfKeyNaturalOrder(
            boolean reversed )
    {
        Comparator<P> priority = new NaturalPriority<P>( reversed );
        return new PriorityMap<K, K, P>( SELF_KEY, priority );
    }

    private final Converter<K, E> keyFunction;
    private final Comparator<P> order;

    private PriorityMap( Converter<K, E> key, Comparator<P> priority )
    {
        this.keyFunction = key;
        this.order = priority;
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
        {
            if ( priority.equals( node.priority ) )
            {
                node.head = new Link<E>( entity, node.head );
                result = true;
            }
            else if ( order.compare( priority, node.priority ) < 0 )
            {
                queue.remove( node );
                put( entity, priority, key );
                result = true;
            }
        }
        else
        {
            put( entity, priority, key );
            result = true;
        }
        return result;
    }
    
    private void put( E entity, P priority, K key )
    {
        Node<E, P> node = new Node<E, P>( entity, priority );
        map.put( key, node );
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
        if ( node == null ) return null;
        return node.priority;
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
            return null;
        }
        else if ( node.head.next == null )
        {
            node = queue.poll();
            map.remove( keyFunction.convert( node.head.entity ) );
            result = new Entry<E, P>( node );
        }
        else
        {
            result = new Entry<E, P>( node );
            node.head = node.head.next;
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
                public int compare( Node<E, P> o1, Node<E, P> o2 )
                {
                    return order.compare( o1.priority, o2.priority );
                }
            } );

    private static class Node<E, P>
    {
        Link<E> head;
        final P priority;

        Node( E entity, P priority )
        {
            this.head = new Link<E>( entity, null );
            this.priority = priority;
        }
    }

    private static class Link<E>
    {
        final E entity;
        final Link<E> next;

        Link( E entity, Link<E> next )
        {
            this.entity = entity;
            this.next = next;
        }
    }
}
