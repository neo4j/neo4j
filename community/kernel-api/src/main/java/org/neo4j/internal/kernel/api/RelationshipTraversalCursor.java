/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.kernel.api;

import static org.neo4j.internal.kernel.api.Read.NO_ID;

/**
 * Cursor for traversing the relationships of a single node.
 */
public interface RelationshipTraversalCursor extends RelationshipDataAccessor, Cursor
{
    /**
     * Get the other node, the one that this cursor was not initialized from.
     * <p>
     * Relationship cursors have context, and know which node they are traversing relationships for, making it
     * possible and convenient to access the other node.
     *
     * @param cursor the cursor to use for accessing the other node.
     */
    void otherNode( NodeCursor cursor );

    long otherNodeReference();

    long originNodeReference();

    RelationshipTraversalCursor EMPTY = new RelationshipTraversalCursor()
    {
        @Override
        public void otherNode( NodeCursor cursor )
        {
        }

        @Override
        public long otherNodeReference()
        {
            return NO_ID;
        }

        @Override
        public long originNodeReference()
        {
            return NO_ID;
        }

        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void setTracer( KernelReadTracer tracer )
        {
        }

        @Override
        public void removeTracer()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public void closeInternal()
        {
        }

        @Override
        public boolean isClosed()
        {
            return true;
        }

        @Override
        public void setCloseListener( CloseListener closeListener )
        {
        }

        @Override
        public void setToken( int token )
        {
        }

        @Override
        public int getToken()
        {
            return TokenRead.ANY_RELATIONSHIP_TYPE;
        }

        @Override
        public long relationshipReference()
        {
            return NO_ID;
        }

        @Override
        public int type()
        {
            return TokenRead.ANY_RELATIONSHIP_TYPE;
        }

        @Override
        public void source( NodeCursor cursor )
        {
        }

        @Override
        public void target( NodeCursor cursor )
        {
        }

        @Override
        public void properties( PropertyCursor cursor )
        {
        }

        @Override
        public long sourceNodeReference()
        {
            return NO_ID;
        }

        @Override
        public long targetNodeReference()
        {
            return NO_ID;
        }

        @Override
        public long propertiesReference()
        {
            return NO_ID;
        }

        @Override
        public String toString()
        {
            return "I AM EMPTY";
        }
    };
}
