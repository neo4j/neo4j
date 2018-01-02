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
package org.neo4j.kernel.impl.util;

import static org.neo4j.kernel.impl.traversal.TraversalTestBase.assertContains;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

public class TestCopyOnWriteHashMap
{
    @Test
    public void keySetUnaffectedByChanges() throws Exception
    {
        Map<Integer, String> map = new CopyOnWriteHashMap<Integer, String>();
        map.put( 0, "0" );
        map.put( 1, "1" );
        map.put( 2, "2" );
        
        assertContains( map.keySet(), 0, 1, 2 );
        
        Iterator<Integer> keys = map.keySet().iterator();
        map.remove( 1 );
        assertContains( keys, 0, 1, 2 );
    }

    @Test
    public void entrySetUnaffectedByChanges() throws Exception
    {
        Map<Integer, String> map = new CopyOnWriteHashMap<Integer, String>();
        map.put( 0, "0" );
        map.put( 1, "1" );
        map.put( 2, "2" );
        @SuppressWarnings( "unchecked" )
        Map.Entry<Integer, String>[] allEntries = map.entrySet().toArray( new Map.Entry[0] );
        
        assertContains( map.entrySet(), allEntries );
        
        Iterator<Entry<Integer, String>> entries = map.entrySet().iterator();
        map.remove( 1 );
        assertContains( entries, allEntries );
    }
}
