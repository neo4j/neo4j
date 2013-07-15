package org.neo4j.helpers.collection;

import java.util.Iterator;

import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

@SuppressWarnings("UnusedDeclaration")
public class PrimitiveIteratorUtil
{
    public static Iterator<Long> toJavaIterator( final PrimitiveLongIterator primIterator )
    {
        return new Iterator<Long>()
        {
            @Override
            public boolean hasNext()
            {
                return primIterator.hasNext();
            }

            @Override
            public Long next()
            {
                return primIterator.next();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException(  );
            }
        };
    }
}
