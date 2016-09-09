package org.neo4j.index.btree;

public interface CountPredicate
{
    boolean reachedLimit( int resultCount );

    public static CountPredicate max( int maxCount )
    {
        return ( resultCount ) -> resultCount >= maxCount;
    }

    public static final CountPredicate NO_LIMIT = resultCount -> false;
}
