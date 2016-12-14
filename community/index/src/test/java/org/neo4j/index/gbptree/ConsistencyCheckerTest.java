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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Test;

import org.neo4j.io.pagecache.PageCursor;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.index.gbptree.ConsistencyChecker.assertNoCrashOrBrokenPointerInGSPP;
import static org.neo4j.index.gbptree.GenSafePointer.MIN_GENERATION;

public class ConsistencyCheckerTest
{
    @Test
    public void shouldThrowDescriptiveExceptionOnBrokenGSPP() throws Exception
    {
        // GIVEN
        int pageSize = 256;
        PageCursor cursor = new PageAwareByteArrayCursor( pageSize );
        Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
        TreeNode<MutableLong,MutableLong> treeNode = new TreeNode<>( pageSize, layout );
        long stableGeneration = MIN_GENERATION;
        long crashGeneration = stableGeneration + 1;
        long unstableGeneration = stableGeneration + 2;
        String pointerFieldName = "abc";
        long pointer = 123;

        cursor.next( 0 );
        treeNode.initializeInternal( cursor, stableGeneration, crashGeneration );
        treeNode.setNewGen( cursor, pointer, stableGeneration, crashGeneration );

        // WHEN
        try
        {
            assertNoCrashOrBrokenPointerInGSPP( cursor, stableGeneration, unstableGeneration,
                    pointerFieldName, TreeNode.BYTE_POS_NEWGEN, treeNode );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( pointerFieldName ) );
            assertThat( e.getMessage(), containsString( pointerFieldName ) );
            assertThat( e.getMessage(), containsString( "state=CRASH" ) );
            assertThat( e.getMessage(), containsString( "state=EMPTY" ) );
            assertThat( e.getMessage(), containsString( String.valueOf( pointer ) ) );
        }
    }
}
