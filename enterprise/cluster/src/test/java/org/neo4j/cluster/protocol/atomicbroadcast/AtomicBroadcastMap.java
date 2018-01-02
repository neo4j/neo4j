/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.protocol.atomicbroadcast;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.cluster.protocol.snapshot.Snapshot;
import org.neo4j.cluster.protocol.snapshot.SnapshotProvider;

/**
 * Map that is synced through an Atomic Broadcast protocol
 */
public class AtomicBroadcastMap<K, V>
        implements Map<K, V>
{
    private Map<K, V> map = new ConcurrentHashMap<K, V>();
    private AtomicBroadcast atomicBroadcast;
    private volatile MapCommand lastCommand;
    protected final AtomicBroadcastListener atomicBroadcastListener;
    private final AtomicBroadcastSerializer serializer = new AtomicBroadcastSerializer(new ObjectStreamFactory(), new ObjectStreamFactory());

    public AtomicBroadcastMap( AtomicBroadcast atomicBroadcast, Snapshot snapshot )
    {
        snapshot.setSnapshotProvider( new SnapshotProvider()
        {
            @Override
            public void getState( ObjectOutputStream output )
                    throws IOException
            {
                output.writeObject( new HashMap( map ) );
            }

            @Override
            public void setState( ObjectInputStream input )
                    throws IOException, ClassNotFoundException
            {
                map = new ConcurrentHashMap( (Map<K, V>) input.readObject() );
            }
        } );

        this.atomicBroadcast = atomicBroadcast;
        atomicBroadcastListener = new AtomicBroadcastListener()
        {
            @Override
            public void receive( Payload value )
            {
                try
                {
                    MapCommand command = (MapCommand) serializer.receive( value );
                    command.execute( map );

//                LoggerFactory.getLogger( getClass() ).info(  "Map:"+map );

                    synchronized ( AtomicBroadcastMap.this )
                    {
                        if ( command.equals( lastCommand ) )
                        {
                            lastCommand = null;
                            AtomicBroadcastMap.this.notifyAll();
                        }
                    }
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
                catch ( ClassNotFoundException e )
                {
                    e.printStackTrace();
                }
            }
        };
        atomicBroadcast.addAtomicBroadcastListener( atomicBroadcastListener );
    }

    @Override
    public int size()
    {
        checkUpToDate();
        return map.size();
    }

    @Override
    public boolean isEmpty()
    {
        checkUpToDate();
        return map.isEmpty();
    }

    @Override
    public boolean containsKey( Object key )
    {
        checkUpToDate();
        return map.containsKey( key );
    }

    @Override
    public boolean containsValue( Object value )
    {
        checkUpToDate();
        return map.containsValue( value );
    }

    @Override
    public V get( Object key )
    {
        checkUpToDate();
        return map.get( key );
    }

    @Override
    public V put( K key, V value )
    {
        try
        {
            atomicBroadcast.broadcast( serializer.broadcast( lastCommand = new Put( key, value ) ) );
            return map.get( key );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    public V remove( Object key )
    {
        try
        {
            atomicBroadcast.broadcast( serializer.broadcast( lastCommand = new Remove( key ) ) );
            return map.get( key );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    public void putAll( Map<? extends K, ? extends V> m )
    {
        try
        {
            atomicBroadcast.broadcast( serializer.broadcast( lastCommand = new PutAll( m ) ) );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    public void clear()
    {
        try
        {
            atomicBroadcast.broadcast( serializer.broadcast( lastCommand = new Clear() ) );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( e );
        }
    }

    @Override
    public Set<K> keySet()
    {
        checkUpToDate();
        return map.keySet();
    }

    @Override
    public Collection<V> values()
    {
        checkUpToDate();
        return map.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        checkUpToDate();
        return map.entrySet();
    }

    public void close()
    {
        checkUpToDate();
        atomicBroadcast.removeAtomicBroadcastListener( atomicBroadcastListener );
    }

    private synchronized void checkUpToDate()
    {
        int tries = 100;
        while ( lastCommand != null  && tries > 0)
        {
            try
            {
                this.wait( 5000 );
            }
            catch ( InterruptedException e )
            {
                e.printStackTrace();
            }
            tries--;
        }

        if (tries == 0)
            throw new IllegalStateException( "Timed out waiting for state to be synchronized" );
    }

    public interface MapCommand
    {
        void execute( Map map );
    }

    public static class Put
            implements Serializable, MapCommand
    {
        Object key;
        Object value;

        public Put( Object key, Object value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public void execute( Map map )
        {
            map.put( key, value );
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

            Put put = (Put) o;

            if ( !key.equals( put.key ) )
            {
                return false;
            }
            if ( !value.equals( put.value ) )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = key.hashCode();
            result = 31 * result + value.hashCode();
            return result;
        }

        @Override
        public String toString()
        {
            return "Put: " + key + "=" + value;
        }
    }

    public static class Remove
            implements Serializable, MapCommand
    {
        Object key;

        public Remove( Object key )
        {
            this.key = key;
        }

        @Override
        public void execute( Map map )
        {
            map.remove( key );
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

            Remove remove = (Remove) o;

            if ( !key.equals( remove.key ) )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return key.hashCode();
        }
    }

    public static class Clear
            implements Serializable, MapCommand
    {
        @Override
        public void execute( Map map )
        {
            map.clear();
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

            return true;
        }
    }

    public static class PutAll
            implements Serializable, MapCommand
    {
        Map map;

        public PutAll( Map map )
        {
            this.map = map;
        }

        @Override
        public void execute( Map map )
        {
            map.putAll( this.map );
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

            PutAll putAll = (PutAll) o;

            if ( !map.equals( putAll.map ) )
            {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return map.hashCode();
        }
    }
}
