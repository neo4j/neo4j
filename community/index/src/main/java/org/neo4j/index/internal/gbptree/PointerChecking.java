/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.index.internal.gbptree;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Methods for ensuring a read {@link GenerationSafePointer GSP pointer} is valid.
 */
class PointerChecking
{
    static final String WRITER_TRAVERSE_OLD_STATE_MESSAGE =
                    "Writer traversed to a tree node that has a valid successor, " +
                    "This is most likely due to failure to checkpoint the tree before shutdown and/or tree state " +
                    "being out of date.";

    private PointerChecking()
    {
    }

    /**
     * Checks a read pointer for success/failure and throws appropriate exception with failure information
     * if failure. Must be called after a consistent read from page cache (after {@link PageCursor#shouldRetry()}.
     *
     * @param result result from {@link GenerationSafePointerPair#FLAG_READ} or
     * {@link GenerationSafePointerPair#write(PageCursor, long, long, long)}.
     * @param allowNoNode If {@link TreeNode#NO_NODE_FLAG} is allowed as pointer value.
     */
    static void checkPointer( long result, boolean allowNoNode )
    {
        GenerationSafePointerPair.assertSuccess( result );
        if ( allowNoNode && !TreeNode.isNode( result ) )
        {
            return;
        }
        if ( result < IdSpace.MIN_TREE_NODE_ID )
        {
            throw new TreeInconsistencyException( "Pointer to id " + result + " not allowed. Minimum node id allowed is " +
                    IdSpace.MIN_TREE_NODE_ID );
        }
    }

    /**
     * Assert cursor rest on a node that does not have a valid (not crashed) successor.
     *
     * @param cursor PageCursor resting on a tree node.
     * @param stableGeneration Current stable generation of tree.
     * @param unstableGeneration Current unstable generation of tree.
     */
    static boolean assertNoSuccessor( PageCursor cursor, long stableGeneration, long unstableGeneration )
    {
        long successor = TreeNode.successor( cursor, stableGeneration, unstableGeneration );
        if ( TreeNode.isNode( successor ) )
        {
            throw new TreeInconsistencyException( WRITER_TRAVERSE_OLD_STATE_MESSAGE );
        }
        return true;
    }
}
