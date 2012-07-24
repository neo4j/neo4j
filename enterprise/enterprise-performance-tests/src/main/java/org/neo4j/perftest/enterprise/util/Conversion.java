package org.neo4j.perftest.enterprise.util;

public interface Conversion<FROM, TO>
{
    TO convert( FROM source );

    public static final Conversion<Number, Integer> TO_INTEGER = new Conversion<Number, Integer>()
    {
        @Override
        public Integer convert( Number source )
        {
            return source.intValue();
        }
    };
}
