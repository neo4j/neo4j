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
    @Test
    public void shouldThrowOnNoNode() throws Exception
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
    public void shouldThrowOnReadFailure() throws Exception
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( GenSafePointerPair.SIZE );
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
    public void shouldThrowOnWriteFailure() throws Exception
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( GenSafePointerPair.SIZE );
        int firstGeneration = 1;
        int secondGeneration = 2;
        int thirdGeneration = 3;
        write( cursor, 123, 0, firstGeneration );
        cursor.setOffset( 0 );
        write( cursor, 456, firstGeneration, secondGeneration );
        cursor.setOffset( 0 );

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
    public void shouldPassOnReadSuccess() throws Exception
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( GenSafePointerPair.SIZE );
        long generation = 1;
        PointerChecking.checkChildPointer( write( cursor, 123, 0, generation ) );
        cursor.setOffset( 0 );

        // WHEN
        long result = read( cursor, 0, generation );

        // THEN
        PointerChecking.checkChildPointer( result );
    }

    @Test
    public void shouldPassOnWriteSuccess() throws Exception
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( GenSafePointerPair.SIZE );
        long generation = 1;

        // WHEN
        long result = write( cursor, 123, 0, generation );

        // THEN
        PointerChecking.checkChildPointer( result );
    }
}
