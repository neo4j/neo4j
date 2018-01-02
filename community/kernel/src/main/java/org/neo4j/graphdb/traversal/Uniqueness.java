/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.traversal;

/**
 * A catalog of convenient uniqueness factories.
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

        public boolean eagerStartBranches()
        {
            return true;
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

        public boolean eagerStartBranches()
        {
            return true;
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

        public boolean eagerStartBranches()
        {
            return true;
        }
    },
    /**
     * Entities on the same level are guaranteed to be unique.
     */
    NODE_LEVEL
    {
        @Override
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptNull( optionalParameter );
            return new LevelUnique( PrimitiveTypeFetcher.NODE );
        }

        public boolean eagerStartBranches()
        {
            return true;
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

        public boolean eagerStartBranches()
        {
            return true;
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

        public boolean eagerStartBranches()
        {
            return false;
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

        public boolean eagerStartBranches()
        {
            return true;
        }
    },
    /**
     * Entities on the same level are guaranteed to be unique.
     */
    RELATIONSHIP_LEVEL
    {
        @Override
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptNull( optionalParameter );
            return new LevelUnique( PrimitiveTypeFetcher.RELATIONSHIP );
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    },

    /**
     * No restriction (the user will have to manage it).
     */
    NONE
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            acceptNull( optionalParameter );
            return notUniqueInstance;
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    };

    private static final UniquenessFilter notUniqueInstance = new NotUnique();

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
