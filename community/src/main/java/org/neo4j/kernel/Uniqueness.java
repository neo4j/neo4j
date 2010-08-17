package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.graphdb.traversal.UniquenessFilter;

/**
 * Contains some uniqueness modes that are very common in traversals, for
 * example uniqueness of nodes or relationships to visit during a traversal.
 */
public enum Uniqueness implements UniquenessFactory
{
    /**
     * A node cannot be traversed more than once. This is what the legacy
     * traversal framework does.
     */
    NODE_GLOBAL
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptNull( optionalParameter );
            return new GloballyUnique( PrimitiveTypeFetcher.NODE );
        }
    },
    /**
     * For each returned node there's a unique path from the start node to it.
     */
    NODE_PATH
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptNull( optionalParameter );
            return new PathUnique( PrimitiveTypeFetcher.NODE );
        }
    },
    /**
     * This is like {@link Uniqueness#NODE_GLOBAL}, but only guarantees
     * uniqueness among the most recent visited nodes, with a configurable
     * count. Traversing a huge graph is quite memory intensive in that it keeps
     * track of <i>all</i> the nodes it has visited. For huge graphs a traverser
     * can hog all the memory in the JVM, causing {@link OutOfMemoryError}.
     * Together with this {@link Uniqueness} you can supply a count, which is
     * the number of most recent visited nodes. This can cause a node to be
     * visited more than once, but scales infinitely.
     */
    NODE_RECENT
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptIntegerOrNull( optionalParameter );
            return new RecentlyUnique( PrimitiveTypeFetcher.NODE, optionalParameter );
        }
    },
    /**
     * A relationship cannot be traversed more than once, whereas nodes can.
     */
    RELATIONSHIP_GLOBAL
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptNull( optionalParameter );
            return new GloballyUnique( PrimitiveTypeFetcher.RELATIONSHIP );
        }
    },
    /**
     * For each returned node there's a (relationship wise) unique path from the
     * start node to it.
     */
    RELATIONSHIP_PATH
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptNull( optionalParameter );
            return new PathUnique( PrimitiveTypeFetcher.RELATIONSHIP );
        }
    },
    /**
     * Same as for {@link Uniqueness#NODE_RECENT}, but for relationships.
     */
    RELATIONSHIP_RECENT
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptIntegerOrNull( optionalParameter );
            return new RecentlyUnique( PrimitiveTypeFetcher.RELATIONSHIP, optionalParameter );
        }
    },
    /**
     * No restriction (the user will have to manage it).
     */
    NONE
    {
        private UniquenessFilter instance = new NotUnique();
        
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptNull( optionalParameter );
            return instance;
        }
    };
    
    private static void acceptNull( Object optionalParameter )
    {
        if ( optionalParameter != null )
        {
            throw new IllegalArgumentException( "Only accepts null parameter, was " +
                    optionalParameter );
        }
    }
    
    private static void acceptIntegerOrNull( Object parameter )
    {
        if ( parameter == null )
        {
            return;
        }
        boolean isDecimalNumber = parameter instanceof Number
                && !( parameter instanceof Float || parameter instanceof Double );
        if ( !isDecimalNumber )
        {
            throw new IllegalArgumentException( "Doesn't accept non-decimal values"
                    + ", like '" + parameter + "'" );
        }
    }
}
