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

import static org.junit.Assert.assertEquals;

public class TreeNodeDeltaTest extends TreeNodeTestBase
{
    @Override
    protected TreeNode<MutableLong,MutableLong> instantiateTreeNode( int pageSize, Layout<MutableLong,MutableLong> layout )
    {
        return new TreeNodeDelta<>( pageSize, layout );
    }

    @Test
    public void shouldInsertDeltaKeys() throws Exception
    {
        // GIVEN
        MutableLong key = new MutableLong();
        key.setValue( 4 );
        deltaSection.insertKeyAt( cursor, key, 0, 0 );
        key.setValue( 7 );
        deltaSection.insertKeyAt( cursor, key, 1, 1 );
        key.setValue( 10 );
        deltaSection.insertKeyAt( cursor, key, 2, 2 );
        deltaSection.setKeyCount( cursor, 3 );

        // WHEN
        key.setValue( 5 );
        deltaSection.insertKeyAt( cursor, key, 1, 3 );
        deltaSection.setKeyCount( cursor, 4 );

        // THEN
        assertEquals( 4, deltaSection.keyAt( cursor, key, 0 ).longValue() );
        assertEquals( 5, deltaSection.keyAt( cursor, key, 1 ).longValue() );
        assertEquals( 7, deltaSection.keyAt( cursor, key, 2 ).longValue() );
        assertEquals( 10, deltaSection.keyAt( cursor, key, 3 ).longValue() );
    }

    @Test
    public void shouldRemoveDeltaKeys() throws Exception
    {
        // GIVEN
        MutableLong key = new MutableLong();
        key.setValue( 4 );
        deltaSection.insertKeyAt( cursor, key, 0, 0 );
        key.setValue( 7 );
        deltaSection.insertKeyAt( cursor, key, 1, 1 );
        key.setValue( 10 );
        deltaSection.insertKeyAt( cursor, key, 2, 2 );
        key.setValue( 11 );
        deltaSection.insertKeyAt( cursor, key, 3, 3 );
        deltaSection.setKeyCount( cursor, 4 );

        // WHEN
        deltaSection.removeKeyAt( cursor, 1, 4 );
        deltaSection.setKeyCount( cursor, 3 );

        // THEN
        assertEquals( 4, deltaSection.keyAt( cursor, key, 0 ).longValue() );
        assertEquals( 10, deltaSection.keyAt( cursor, key, 1 ).longValue() );
        assertEquals( 11, deltaSection.keyAt( cursor, key, 2 ).longValue() );
    }

    @Test
    public void shouldInsertDeltaValues() throws Exception
    {
        // GIVEN
        MutableLong value = new MutableLong();
        value.setValue( 4 );
        deltaSection.insertValueAt( cursor, value, 0, 0 );
        value.setValue( 7 );
        deltaSection.insertValueAt( cursor, value, 1, 1 );
        value.setValue( 10 );
        deltaSection.insertValueAt( cursor, value, 2, 2 );
        deltaSection.setKeyCount( cursor, 3 );

        // WHEN
        value.setValue( 5 );
        deltaSection.insertValueAt( cursor, value, 1, 3 );
        deltaSection.setKeyCount( cursor, 4 );

        // THEN
        assertEquals( 4, deltaSection.valueAt( cursor, value, 0 ).longValue() );
        assertEquals( 5, deltaSection.valueAt( cursor, value, 1 ).longValue() );
        assertEquals( 7, deltaSection.valueAt( cursor, value, 2 ).longValue() );
        assertEquals( 10, deltaSection.valueAt( cursor, value, 3 ).longValue() );
    }

    @Test
    public void shouldRemoveDeltaValues() throws Exception
    {
        // GIVEN
        MutableLong value = new MutableLong();
        value.setValue( 4 );
        deltaSection.insertValueAt( cursor, value, 0, 0 );
        value.setValue( 7 );
        deltaSection.insertValueAt( cursor, value, 1, 1 );
        value.setValue( 10 );
        deltaSection.insertValueAt( cursor, value, 2, 2 );
        value.setValue( 11 );
        deltaSection.insertValueAt( cursor, value, 3, 3 );
        deltaSection.setKeyCount( cursor, 4 );

        // WHEN
        deltaSection.removeValueAt( cursor, 1, 4 );
        deltaSection.setKeyCount( cursor, 3 );

        // THEN
        assertEquals( 4, deltaSection.valueAt( cursor, value, 0 ).longValue() );
        assertEquals( 10, deltaSection.valueAt( cursor, value, 1 ).longValue() );
        assertEquals( 11, deltaSection.valueAt( cursor, value, 2 ).longValue() );
    }
}
