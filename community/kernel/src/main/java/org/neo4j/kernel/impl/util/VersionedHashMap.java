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
package org.neo4j.kernel.impl.util;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static java.lang.Integer.bitCount;

/**
 * A single-threaded map with approximate 1-1 performance mapping to {@link java.util.HashMap}. Memory characteristics
 * are in the same order, but this uses 6 bytes more memory per entry stored.
 *
 * In return, it gives some special characteristics for iterating over and manipulating its contents simultaneously.
 *
 * <h2>Behavior for adding entries while iterating</h2>
 *
 * If you add entries while iterating over this map, those added entries will not be returned by any
 * iterator created before the addition. This allows iterating over the map and adding entries without creating an
 * infinite loop.
 *
 * <h2>Behavior for removing entries while iterating</h2>
 *
 * Removing entries does not behave the same way. Assuming you always call next() immediately after calling hasNext(),
 * removing entries in the map while you iterate will have those changes made visible in your current iterator. In
 * other words, if you remove entries while iterating, those entries will not be returned by your iterator either.
 *
 * If you call hasNext() and then remove entries from the map before calling next(), behavior is undefined. In reality,
 * you may see the removed entry, if that removed entry was the one set to be returned next.
 *
 * Therefore, avoid modifying the map between calls to hasNext() and next(). Outside of that boundary, behavior will be
 * well defined.
 *
 * <h2>How it works</h2>
 *
 * Internally, this data structures uses MVCC to track added records and exclude them in iterators that were created
 * before the new record. Removed records are marked as removed, and usually garbage collected right away unless an
 * iterator holds a reference to a removed record.
 *
 * <h2>Memory considerations</h2>
 *
 * Iterators created before a very large number of inserts will pay a penalty which may be substantial in the
 * right conditions. If your use case implies returning an iterator and then handling very large numbers of inserts
 * before that iterator is used, you may want to consider an alternative data structure.
 *
 * Similarily, iterators created before a very large number of inserts will incur a penalty on the whole data structure,
 * as it requires copy-on-write behavior for some records (specifically records that have "next" records).
 */
public class VersionedHashMap<K, V> implements Map<K, V>
{
    private static final int MAX_BUCKETS = 1 << 30;

    /**
     * Contains "buckets" of hash chains. Each entry in this array points to a linked-list type data structure that
     * make up chains of values with hashes for the relevant bucket.
     *
     * See http://en.wikipedia.org/wiki/Hash_table
     */
    private Record<K, V>[] buckets;

    private int size = 0;
    private int version = 0;

    /**
     * Number of current non-exhausted iterators. When we know there are iterators pending we need to take special
     * precautions during resizing so as to not screw up those iterators state. Specifically, those special precautions
     * mean that we copy records that are in a chain, rather than move them, so as to not upset the iterator positions.
     *
     * Ideally, this copying will be very rare, as most records will not be in chains, so this should not incur huge
     * overhead.
     */
    private short liveIterators = 0;

    private int bitwiseModByBuckets;

    private int resizeThreshold;
    private float resizeAtCapacity;

    private final EntrySet entrySet = new EntrySet();
    private final KeySet keys = new KeySet();
    private final ValueCollection values = new ValueCollection();

    public VersionedHashMap()
    {
        this(16, 0.85f);
    }

    public VersionedHashMap( int numBuckets, float resizeAtCapacity )
    {
        if( bitCount( numBuckets ) != 1)
        {
            throw new UnsupportedOperationException( "Number of buckets must be a power-of-2 number, 2,4,8,16 etc." );
        }
        this.resizeAtCapacity = resizeAtCapacity;
        this.buckets = new Record[numBuckets];
        this.bitwiseModByBuckets = numBuckets - 1;
        this.resizeThreshold = (int)(numBuckets * resizeAtCapacity);
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public boolean isEmpty()
    {
        return size == 0;
    }

    @Override
    public boolean containsKey( Object key )
    {
        return getRecord( key ) != null;
    }

    @Override
    public boolean containsValue( Object value )
    {
        for ( Record<K, V> bucket : buckets )
        {
            while(bucket != null)
            {
                if(bucket.value == value || bucket.value.equals( value ))
                {
                    return true;
                }
                bucket = bucket.next;
            }
        }

        return false;
    }

    @Override
    public V get( Object key )
    {
        Record<K, V> record = getRecord( key );
        if(record != null)
        {
            return record.value;
        }
        return null;
    }

    @Override
    public V put( K key, V value )
    {
        final int hash = hash(key.hashCode());
        final int bucket = hash & bitwiseModByBuckets;

        for(Record<K,V> record = buckets[bucket]; record != null; record = record.next)
        {
            if(record.hashCode == hash && ((record.key == key) || record.key.equals( key )))
            {
                V old = record.value;
                record.value = value;
                return old;
            }
        }

        // No pre-existing entry, create a new one
        Record<K,V> record = new Record<>(hash, key, value, buckets[bucket], version);
        buckets[bucket] = record;

        if(size++ > resizeThreshold)
        {
            resize( buckets.length << 1 );
        }

        return null;
    }

    @Override
    public V remove( Object key )
    {
        final int hash = hash(key.hashCode());
        final int bucket = hash & bitwiseModByBuckets;

        Record<K, V> prev = null;
        for(Record<K,V> record = buckets[bucket]; record != null; record = record.next)
        {
            if(record.hashCode == hash && ((record.key == key) || record.key.equals( key )))
            {
                V old = record.value;
                if(prev == null)
                {
                    buckets[bucket] = record.next;
                }
                else
                {
                    prev.next = record.next;
                }
                record.remove();
                size--;
                return old;
            }
            prev = record;
        }

        return null;
    }

    @Override
    public void putAll( Map<? extends K, ? extends V> m )
    {
        throw new UnsupportedOperationException( "Not yet implemented." );
    }

    @Override
    public void clear()
    {
        size = 0;
        for ( int i = 0; i < buckets.length; i++ )
        {
            buckets[i] = null;
        }
    }

    @Override
    public Set<K> keySet()
    {
        return keys;
    }

    @Override
    public Collection<V> values()
    {
        return values;
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet()
    {
        return entrySet;
    }

    private Record<K,V> getRecord( Object key )
    {
        final int hash = hash(key.hashCode());
        final int bucket = hash & bitwiseModByBuckets;

        for(Record<K,V> record = buckets[bucket]; record != null; record = record.next)
        {
            if(record.key.equals( key ))
            {
                return record;
            }
        }

        return null;
    }

    private void resize(int numBuckets)
    {
        if(numBuckets >= MAX_BUCKETS)
        {
            // Avoid getting this call again, we can't make it any bigger.
            resizeThreshold = Integer.MAX_VALUE;
            return;
        }

        Record<K,V>[] oldBuckets = buckets;

        buckets = new Record[numBuckets];
        bitwiseModByBuckets = numBuckets - 1;
        resizeThreshold = (int)(numBuckets * resizeAtCapacity);

        for ( Record<K, V> record : oldBuckets )
        {
            while(record != null)
            {
                Record<K,V> next = record.next;

                if(next != null && liveIterators > 0)
                {
                    record = record.copy();
                }
                else if(liveIterators == 0 && record instanceof CopiedRecord)
                {
                    // If there are no iterators, take this opportunity to get rid of copied records and use the
                    // originals.
                    record = ((CopiedRecord)record).original;
                }

                int bucket = record.hashCode & bitwiseModByBuckets;
                record.next = buckets[bucket];
                buckets[bucket] = record;
                record = next;
            }
        }

    }

    private static int hash(int h)
    {
        // See: http://stackoverflow.com/questions/9335169/understanding-strange-java-hash-function
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    private class EntrySet extends AbstractSet<Entry<K,V>>
    {
        @Override
        public Iterator<Entry<K,V>> iterator()
        {
            return new EntryIterator();
        }

        @Override
        public boolean add( Entry<K, V> kvEntry )
        {
            return put( kvEntry.getKey(), kvEntry.getValue() ) != null;
        }

        @Override
        public int size()
        {
            return size;
        }
    }

    private class KeySet extends AbstractSet<K>
    {
        @Override
        public Iterator<K> iterator()
        {
            return new KeyIterator( entrySet.iterator() );
        }

        @Override
        public int size()
        {
            return size;
        }
    }

    private class ValueCollection extends AbstractCollection<V>
    {
        @Override
        public Iterator<V> iterator()
        {
            return new ValueIterator( entrySet.iterator() );
        }

        @Override
        public int size()
        {
            return size;
        }
    }

    private class EntryIterator implements Iterator<Entry<K,V>>
    {
        private int viewVersion;
        private int currentBucket = 0;
        private Record<K,V> next;
        private Record<K,V> current;

        // In case the map is resized, we need to retain a fixed view of the buckets, so keep a reference to the current
        // bucket array.
        private Record<K,V>[] bucketsView = buckets;
        private boolean exhausted = false;

        private EntryIterator()
        {
            viewVersion = version;
            version++;
            liveIterators++;

            // Find first entry
            for ( ; next == null && currentBucket < bucketsView.length; currentBucket++ )
            {
                next = bucketsView[currentBucket];
            }
        }

        @Override
        public boolean hasNext()
        {
            if(exhausted)
            {
                return false;
            }

            // Take into account the fact that we may have pre-fetched a record that has then been removed
            if( next != null && next.removed )
            {
                next();
            }

            if(next != null)
            {
                return true;
            }
            else
            {
                exhausted = true;
                liveIterators--;
                return false;
            }
        }

        @Override
        public Record<K, V> next()
        {
            current = next;

            // This is rather complex, but the gist is this: Iterate over each bucket, and within each iterate over
            // the chain of records. If we find a record that we are allowed to see
            // (eg. as an addedInVersion <= viewVersion && !removed), stop and set next to that record.
            if((next = next.next) == null || next.addedInVersion > viewVersion || next.removed)
            {
                for ( ; (next == null || next.addedInVersion > viewVersion || next.removed)
                        && currentBucket < bucketsView.length; currentBucket++ )
                {
                    next = bucketsView[currentBucket];
                    while( next != null && (next.addedInVersion > viewVersion || next.removed))
                    {
                        next = next.next;
                    }
                }
            }

            return current;
        }

        @Override
        public void remove()
        {
            if(current == null)
            {
                throw new IllegalStateException( "Not currently on a record. Did you call next()?" );
            }
            VersionedHashMap.this.remove( current.key );
        }
    }

    private class KeyIterator implements Iterator<K>
    {
        private final Iterator<Entry<K,V>> entryIterator;

        private KeyIterator(Iterator<Entry<K,V>> entryIterator)
        {
            this.entryIterator = entryIterator;
        }

        @Override
        public boolean hasNext()
        {
            return entryIterator.hasNext();
        }

        @Override
        public K next()
        {
            return entryIterator.next().getKey();
        }

        @Override
        public void remove()
        {
            entryIterator.remove();
        }
    }

    private class ValueIterator implements Iterator<V>
    {
        private final Iterator<Entry<K,V>> entryIterator;

        private ValueIterator(Iterator<Entry<K,V>> entryIterator)
        {
            this.entryIterator = entryIterator;
        }

        @Override
        public boolean hasNext()
        {
            return entryIterator.hasNext();
        }

        @Override
        public V next()
        {
            return entryIterator.next().getValue();
        }

        @Override
        public void remove()
        {
            entryIterator.remove();
        }
    }

    private static class Record<K, V> implements Entry<K,V>
    {
        protected int hashCode;
        protected K key;
        protected V value;
        protected Record<K, V> next;

        protected int addedInVersion;
        protected boolean removed = false;

        public Record(int hashCode, K key, V value, Record<K,V> next, int addedInVersion)
        {
            this.hashCode = hashCode;
            this.key = key;
            this.value = value;
            this.next = next;
            this.addedInVersion = addedInVersion;
        }

        @Override
        public K getKey()
        {
            return key;
        }

        @Override
        public V getValue()
        {
            return value;
        }

        @Override
        public V setValue( V value )
        {
            V old = this.value;
            this.value = value;
            return old;
        }

        public void remove()
        {
            removed = true;
        }

        public Record<K,V> copy()
        {
            if(!removed)
            {
                return new CopiedRecord<>( this, hashCode, key, value, next, addedInVersion );
            }
            else
            {
                return new Record<>( hashCode, key, value, next, addedInVersion );
            }
        }

        @Override
        public String toString()
        {
            return "Record{" +
                    "hashCode=" + hashCode +
                    ", key=" + key +
                    ", value=" + value +
                    ", next=" + (next == null ? "null + " : next.key) +
                    ", addedInVersion=" + addedInVersion +
                    ", removed=" + removed +
                    '}';
        }
    }

    /**
     * This is used when resizing, and we need to copy records that are part of chains, so that existing iterators are
     * not messed up. In order for this to work, the copies (which become part of the "real" chain) need to refer back
     * to their originals, so that they can be removed and those removals be visible to iterators. Without this,
     * iterators would still show removed records if they were removed after a resize.
     */
    private static class CopiedRecord<K,V> extends Record<K,V>
    {
        private Record<K,V> original;

        public CopiedRecord(Record<K,V> original, int hashCode, K key, V value, Record<K,V> next, int addedInVersion)
        {
            super(hashCode, key, value, next, addedInVersion);
            this.original = original;
        }

        @Override
        public void remove()
        {
            removed = true;
            original.remove();
        }

        @Override
        public String toString()
        {
            return "CopiedRecord{" +
                    "hashCode=" + hashCode +
                    ", key=" + key +
                    ", value=" + value +
                    ", next=" + (next == null ? "null + " : next.key) +
                    ", addedInVersion=" + addedInVersion +
                    ", removed=" + removed +
                    ", original=" + original +
                    '}';
        }
    }
}
