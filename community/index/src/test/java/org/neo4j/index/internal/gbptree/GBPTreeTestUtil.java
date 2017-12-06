package org.neo4j.index.internal.gbptree;

import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class GBPTreeTestUtil
{
    static <KEY> boolean contains( List<KEY> expectedKeys, KEY key, Comparator<KEY> comparator )
    {
        return expectedKeys.stream()
                .map( bind( comparator::compare, key ) )
                .anyMatch( Predicate.isEqual( 0 ) );
    }

    private static <T, U, R> Function<U,R> bind( BiFunction<T,U,R> f, T t )
    {
        return u -> f.apply( t, u );
    }
}
