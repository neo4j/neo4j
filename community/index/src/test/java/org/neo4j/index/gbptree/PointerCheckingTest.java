/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.gbptree;

import org.junit.Test;

import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.fail;

import static org.neo4j.index.gbptree.GenSafePointerPair.read;
import static org.neo4j.index.gbptree.GenSafePointerPair.write;

public class PointerCheckingTest
{
    private final PageCursor cursor = ByteArrayPageCursor.wrap( GenSafePointerPair.SIZE );
    private final int firstGeneration = 1;
    private final int secondGeneration = 2;
    private final int thirdGeneration = 3;

    @Test
    public void checkChildShouldThrowOnNoNode() throws Exception
    {
        // WHEN
        try
        {
            PointerChecking.checkChildPointer( TreeNode.NO_NODE_FLAG );
            fail( "Should have failed ");
        }
        catch ( IllegalStateException e )
        {
            // THEN good
        }
    }

    @Test
    public void checkChildShouldThrowOnReadFailure() throws Exception
    {
        // GIVEN
        long result = GenSafePointerPair.read( cursor, 0, 1 );

        // WHEN
        try
        {
            PointerChecking.checkChildPointer( result );
            fail( "Should have failed ");
        }
        catch ( IllegalStateException e )
        {
            // THEN good
        }
    }

    @Test
    public void checkChildShouldThrowOnWriteFailure() throws Exception
    {
        // GIVEN
        write( cursor, 123, 0, firstGeneration );
        cursor.rewind();
        write( cursor, 456, firstGeneration, secondGeneration );
        cursor.rewind();

        // WHEN
        // This write will see first and second written pointers and think they belong to CRASHed generation
        long result = write( cursor, 789, 0, thirdGeneration );
        try
        {
            PointerChecking.checkChildPointer( result );
            fail( "Should have failed ");
        }
        catch ( IllegalStateException e )
        {
            // THEN good
        }
    }

    @Test
    public void checkChildShouldPassOnReadSuccess() throws Exception
    {
        // GIVEN
        PointerChecking.checkChildPointer( write( cursor, 123, 0, firstGeneration ) );
        cursor.rewind();

        // WHEN
        long result = read( cursor, 0, firstGeneration );

        // THEN
        PointerChecking.checkChildPointer( result );
    }

    @Test
    public void checkChildShouldPassOnWriteSuccess() throws Exception
    {
        // WHEN
        long result = write( cursor, 123, 0, firstGeneration );

        // THEN
        PointerChecking.checkChildPointer( result );
    }

    @Test
    public void checkSiblingShouldPassOnReadSuccessForNoNodePointer() throws Exception
    {
        // GIVEN
        write( cursor, TreeNode.NO_NODE_FLAG, firstGeneration, secondGeneration );
        cursor.rewind();

        // WHEN
        long result = read( cursor, firstGeneration, secondGeneration );

        // THEN
        PointerChecking.checkSiblingPointer( result );
    }

    @Test
    public void checkSiblingShouldPassOnReadSuccessForNodePointer() throws Exception
    {
        // GIVEN
        long pointer = 101;
        write( cursor, pointer, firstGeneration, secondGeneration );
        cursor.rewind();

        // WHEN
        long result = read( cursor, firstGeneration, secondGeneration );

        // THEN
        PointerChecking.checkSiblingPointer( result );
    }

    @Test
    public void checkSiblingShouldThrowOnReadFailure() throws Exception
    {
        // WHEN
        long result = read( cursor, firstGeneration, secondGeneration );

        // WHEN
        try
        {
            PointerChecking.checkSiblingPointer( result );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // THEN good
        }
    }

    @Test
    public void checkSiblingShouldThrowOnReadIllegalPointer() throws Exception
    {
        // GIVEN
        GenSafePointer.write( cursor, secondGeneration, IdSpace.STATE_PAGE_A );
        cursor.rewind();

        // WHEN
        long result = read( cursor, firstGeneration, secondGeneration );

        // WHEN
        try
        {
            PointerChecking.checkSiblingPointer( result );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // THEN good
        }
    }
}
