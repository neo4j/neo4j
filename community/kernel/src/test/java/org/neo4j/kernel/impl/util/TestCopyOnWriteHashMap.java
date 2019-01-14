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
package org.neo4j.kernel.impl.util;

import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.helpers.collection.Iterators;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TestCopyOnWriteHashMap
{
    @Test
    public void keySetUnaffectedByChanges()
    {
        Map<Integer, String> map = new CopyOnWriteHashMap<>();
        map.put( 0, "0" );
        map.put( 1, "1" );
        map.put( 2, "2" );

        assertThat( map, hasKey( 0 ) );
        assertThat( map, hasKey( 1 ) );
        assertThat( map, hasKey( 2 ) );

        Iterator<Integer> keys = map.keySet().iterator();
        map.remove( 1 );
        List<Integer> keysBeforeDeletion = Iterators.asList( keys );
        assertThat( keysBeforeDeletion, contains( 0, 1, 2 ) );
    }

    @Test
    public void entrySetUnaffectedByChanges()
    {
        Map<Integer, String> map = new CopyOnWriteHashMap<>();
        map.put( 0, "0" );
        map.put( 1, "1" );
        map.put( 2, "2" );
        @SuppressWarnings( "unchecked" )
        Map.Entry<Integer, String>[] allEntries = map.entrySet().toArray( new Map.Entry[0] );

        assertThat( map.entrySet(), containsInAnyOrder( allEntries ) );

        Iterator<Entry<Integer, String>> entries = map.entrySet().iterator();
        map.remove( 1 );
        List<Entry<Integer,String>> entriesBeforeRemoval = Iterators.asList( entries );
        assertThat( entriesBeforeRemoval, containsInAnyOrder( allEntries ) );
    }

    @Test
    public void snapshotShouldKeepData()
    {
        CopyOnWriteHashMap<Integer,String> map = new CopyOnWriteHashMap<>();
        map.put( 0, "0" );
        Map<Integer,String> snapshot = map.snapshot();
        assertThat( snapshot.get( 0 ), is( "0" ) );
        assertThat( map.remove( 0 ), is( "0" ) );
        assertThat( snapshot.get( 0 ), is( "0" ) );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void snapshotMustBeUnmodifiable()
    {
        new CopyOnWriteHashMap<>().snapshot().put( 0, "0" );
    }
}
