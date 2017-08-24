/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ConsistencyCheckerDeltaFormatTest extends ConsistencyCheckerTest
{
    // tests to add for delta format

    @Test
    public void shouldDetectOutOfOrderDeltaKeys() throws Exception
    {
        // given
        node.main().insertKeyAt( cursor, new MutableLong( 3 ), 0, 0 );
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 1, 1 );
        node.main().setKeyCount( cursor, 2 );
        node.delta().insertKeyAt( cursor, new MutableLong( 2 ), 0, 0 );
        node.delta().insertKeyAt( cursor, new MutableLong( 1 ), 1, 1 );
        node.delta().setKeyCount( cursor, 2 );

        // when
        try
        {
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Non-unique key 1" ) );
        }
    }

    @Test
    public void shouldDetectNonUniqueKeysBetweenMainAndDeltaSections() throws Exception
    {
        // given
        node.main().insertKeyAt( cursor, new MutableLong( 3 ), 0, 0 );
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 1, 1 );
        node.main().insertKeyAt( cursor, new MutableLong( 7 ), 2, 2 );
        node.main().setKeyCount( cursor, 3 );
        node.delta().insertKeyAt( cursor, new MutableLong( 4 ), 0, 0 );
        node.delta().insertKeyAt( cursor, new MutableLong( 5 ), 1, 1 );
        node.delta().setKeyCount( cursor, 2 );

        // when
        try
        {
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Non-unique key 5" ) );
        }
    }

    @Test
    public void shouldDetectHighestKeyNotInMain() throws Exception
    {
        // given
        node.main().insertKeyAt( cursor, new MutableLong( 3 ), 0, 0 );
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 1, 1 );
        node.main().insertKeyAt( cursor, new MutableLong( 7 ), 2, 2 );
        node.main().setKeyCount( cursor, 3 );
        node.delta().insertKeyAt( cursor, new MutableLong( 4 ), 0, 0 );
        node.delta().insertKeyAt( cursor, new MutableLong( 8 ), 1, 1 );
        node.delta().setKeyCount( cursor, 2 );

        // when
        try
        {
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Highest key 8 not in main section" ) );
        }
    }
}
