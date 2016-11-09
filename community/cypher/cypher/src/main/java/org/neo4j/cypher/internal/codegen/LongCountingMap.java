package org.neo4j.cypher.internal.codegen;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongLongMap;

public class LongCountingMap implements AutoCloseable
{
    private PrimitiveLongLongMap map = Primitive.offHeapLongLongMap();

    public void update(Long key)
    {
        long current = map.get( key );
        //map.put( object, current + 1L );
    }

    public long getCount(long object)
    {
        return map.get( object );
    }


    @Override
    public void close() throws Exception
    {
        map.close();
    }
}
