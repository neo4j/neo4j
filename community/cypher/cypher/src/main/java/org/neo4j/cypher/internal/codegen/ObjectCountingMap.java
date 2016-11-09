package org.neo4j.cypher.internal.codegen;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ObjectCountingMap
{
    private final HashMap<Object, Long> map = new HashMap<>(  );

    public void update(Object object)
    {
        Long current = map.getOrDefault( object, 0L );
        map.put( object, current + 1L );
    }

    public long getCount(Object object)
    {
        return map.getOrDefault( object, 0L );
    }

    public Iterator<Map.Entry<Object, Long>> iterator()
    {
        return map.entrySet().iterator();
    }
}
