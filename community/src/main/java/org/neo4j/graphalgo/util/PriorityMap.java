package org.neo4j.graphalgo.util;

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
     * Add an entity to the priority map.
     *
     * @param entity the entity to add.
     * @param priority the priority of the entity.
     */
    public void put( E entity, P priority )
    {
        K key = keyFunction.convert( entity );
        Node<E, P> node = map.get( key );
        if ( node != null && priority.equals( node.priority ) )
        {
            node.head = new Link<E>( entity, node.head );
        }
        else
        {
            node = new Node<E, P>( entity, priority );
            map.put( key, node );
            queue.add( node );
        }
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
        if ( node == null )
        {
            return null;
        }
        else if ( node.head.next == null )
        {
            node = queue.poll();
            map.remove( keyFunction.convert( node.head.entity ) );
        }
        else
        {
            node.head = node.head.next;
        }
        return new Entry<E, P>( node );
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
