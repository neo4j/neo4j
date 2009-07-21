package org.neo4j.impl.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

public class SoftValue<K,V> extends SoftReference<V> 
{
    public final K key;
    
    public SoftValue( K key, V value, ReferenceQueue<? super V> queue )
    {
        super( value, queue );
        this.key = key;
    }
}
