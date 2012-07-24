package org.neo4j.perftest.enterprise.util;

public abstract class Predicate<T>
{
    public static Predicate<Long> integerRange( final Long minValue, final Long maxValue )
    {
        return new Predicate<Long>()
        {
            @Override
            boolean matches( Long value )
            {
                return value <= maxValue && value >= minValue;
            }

            @Override
            public String toString()
            {
                return String.format( "%s, %s", minValue == null ? "]-inf" : ("[" + minValue), maxValue == null ? "inf]" : (maxValue + "]") );
            }
        };
    }

    public static Predicate<Long> integerRange( Integer minValue, Integer maxValue )
    {
        return integerRange( minValue == null ? null : minValue.longValue(),
                             maxValue == null ? null : maxValue.longValue() );
    }

    abstract boolean matches( T value );
}
