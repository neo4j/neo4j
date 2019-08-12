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

import org.junit.jupiter.api.Test;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.index.internal.gbptree.GBPTreeGenerationTarget.NO_GENERATION_TARGET;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.write;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.put6BLong;

class PointerCheckingTest
{
    private final PageCursor cursor = ByteArrayPageCursor.wrap( GenerationSafePointerPair.SIZE );
    private final long firstGeneration = 1;
    private final long secondGeneration = 2;
    private final long thirdGeneration = 3;

    @Test
    void checkChildShouldThrowOnNoNode()
    {
        assertThrows(TreeInconsistencyException.class, () -> PointerChecking.checkPointer( TreeNode.NO_NODE_FLAG, false ) );
    }

    @Test
    void checkChildShouldThrowOnReadFailure()
    {
        long result = GenerationSafePointerPair.read( cursor, 0, 1, NO_GENERATION_TARGET );

        assertThrows( TreeInconsistencyException.class, () -> PointerChecking.checkPointer( result, false ) );
    }

    @Test
    void checkChildShouldThrowOnWriteFailure()
    {
        // GIVEN
        write( cursor, 123, 0, firstGeneration );
        cursor.rewind();
        write( cursor, 456, firstGeneration, secondGeneration );
        cursor.rewind();

        // WHEN
        // This write will see first and second written pointers and think they belong to CRASHed generation
        long result = write( cursor, 789, 0, thirdGeneration );
        assertThrows( TreeInconsistencyException.class, () -> PointerChecking.checkPointer( result, false ) );
    }

    @Test
    void checkChildShouldPassOnReadSuccess()
    {
        // GIVEN
        PointerChecking.checkPointer( write( cursor, 123, 0, firstGeneration ), false );
        cursor.rewind();

        // WHEN
        long result = read( cursor, 0, firstGeneration, NO_GENERATION_TARGET );

        // THEN
        PointerChecking.checkPointer( result, false );
    }

    @Test
    void checkChildShouldPassOnWriteSuccess()
    {
        // WHEN
        long result = write( cursor, 123, 0, firstGeneration );

        // THEN
        PointerChecking.checkPointer( result, false );
    }

    @Test
    void checkSiblingShouldPassOnReadSuccessForNoNodePointer()
    {
        // GIVEN
        write( cursor, TreeNode.NO_NODE_FLAG, firstGeneration, secondGeneration );
        cursor.rewind();

        // WHEN
        long result = read( cursor, firstGeneration, secondGeneration, NO_GENERATION_TARGET );

        // THEN
        PointerChecking.checkPointer( result, true );
    }

    @Test
    void checkSiblingShouldPassOnReadSuccessForNodePointer()
    {
        // GIVEN
        long pointer = 101;
        write( cursor, pointer, firstGeneration, secondGeneration );
        cursor.rewind();

        // WHEN
        long result = read( cursor, firstGeneration, secondGeneration, NO_GENERATION_TARGET );

        // THEN
        PointerChecking.checkPointer( result, true );
    }

    @Test
    void checkSiblingShouldThrowOnReadFailure()
    {
        long result = read( cursor, firstGeneration, secondGeneration, NO_GENERATION_TARGET );

        assertThrows( TreeInconsistencyException.class, () -> PointerChecking.checkPointer( result, true ) );
    }

    @Test
    void checkSiblingShouldThrowOnReadIllegalPointer()
    {
        // GIVEN
        long generation = IdSpace.STATE_PAGE_A;
        long pointer = this.secondGeneration;

        // Can not use GenerationSafePointer.write because it will fail on pointer assertion.
        cursor.putInt( (int) pointer );
        put6BLong( cursor, generation );
        cursor.putShort( GenerationSafePointer.checksumOf( generation, pointer ) );
        cursor.rewind();

        // WHEN
        long result = read( cursor, firstGeneration, pointer, NO_GENERATION_TARGET );

        assertThrows(TreeInconsistencyException.class, () -> PointerChecking.checkPointer( result, true ) );
    }
}
