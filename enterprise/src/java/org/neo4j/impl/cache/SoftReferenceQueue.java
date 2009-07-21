package org.neo4j.impl.cache;

import java.lang.ref.ReferenceQueue;

public class SoftReferenceQueue<K,V> extends ReferenceQueue<SoftValue>
{
    public SoftReferenceQueue()
    {
        super();
    }
    
    public SoftValue<K,V> safePoll()
    {
        return (SoftValue) poll();
    }
}
