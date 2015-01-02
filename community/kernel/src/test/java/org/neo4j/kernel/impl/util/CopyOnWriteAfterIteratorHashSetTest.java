/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Test;
import org.neo4j.helpers.collection.IteratorUtil;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class CopyOnWriteAfterIteratorHashSetTest {

    @Test
    public void should_not_change_iterated_snapshot_by_adding() {
        // given
        CopyOnWriteAfterIteratorHashSet<String> set = newCOWSet( "hullo" );
        Snapshot<String> initialSnapshot = snapshot( set );

        // when
        set.add( "wurld" );

        // then
        assertEquals( asSet( "hullo" ), initialSnapshot.toSet());
        assertEquals( asSet( "hullo", "wurld" ), snapshot( set ).toSet() );
    }

    @Test
    public void should_not_change_iterated_snapshot_by_removing() {
        // given
        CopyOnWriteAfterIteratorHashSet<String> set = newCOWSet( "hullo", "wurld" );
        Snapshot<String> initialSnapshot = snapshot( set );

        // when
        set.remove( "wurld" );

        // then
        assertEquals( asSet( "hullo", "wurld" ), initialSnapshot.toSet() );
        assertEquals( asSet( "hullo" ), snapshot( set ).toSet() );
    }

    @Test
    public void should_not_change_iterated_snapshot_by_clearing() {
        // given
        CopyOnWriteAfterIteratorHashSet<String> set = newCOWSet( "hullo", "wurld" );
        Snapshot<String> initialSnapshot = snapshot( set );

        // when
        set.clear();

        // then
        assertEquals( asSet( "hullo", "wurld" ), initialSnapshot.toSet() );
        assertEquals( IteratorUtil.<String>asSet(), snapshot( set ).toSet() );
    }

    @Test
    public void should_support_multiple_stable_snapshots() {
        // given
        CopyOnWriteAfterIteratorHashSet<String> set = newCOWSet();
        set.add( "hullo" );

        // when
        Snapshot<String> snapshot1 = snapshot( set );
        set.add( "wurld" );

        Snapshot<String> snapshot2 = snapshot( set );
        set.add( "!" );
        set.remove( "wurld" );

        Snapshot<String> snapshot3 = snapshot( set );
        set.clear();

        // then
        assertEquals( asSet( "hullo" ), snapshot1.toSet() );
        assertEquals( asSet( "hullo", "wurld" ), snapshot2.toSet() );
        assertEquals( asSet( "hullo", "!" ), snapshot3.toSet() );
        assertEquals( IteratorUtil.<String>asSet(), snapshot( set ).toSet() );
    }

    @Test
    public void should_not_change_iterated_snapshot_by_retaining_all() {
        // given
        CopyOnWriteAfterIteratorHashSet<String> set = newCOWSet( "hullo", "wurld", "!" );
        Snapshot<String> initialSnapshot = snapshot( set );

        // when
        set.retainAll( asList( "!", "!" ) );

        // then
        assertEquals( asSet( "hullo", "wurld", "!" ), initialSnapshot.toSet());
        assertEquals( asSet( "!" ), snapshot( set ).toSet() );
    }

    @Test
    public void should_not_change_iterated_snapshot_by_removing_all() {
        // given
        CopyOnWriteAfterIteratorHashSet<String> set = newCOWSet( "hullo", "wurld", "!" );
        Snapshot<String> initialSnapshot = snapshot( set );

        // when
        set.removeAll(asList("!", "!"));

        // then
        assertEquals (asSet( "hullo", "wurld", "!" ), initialSnapshot.toSet());
        assertEquals( asSet( "hullo", "wurld" ), snapshot( set ).toSet() );
    }

    private CopyOnWriteAfterIteratorHashSet<String> newCOWSet( String... elements ) {
        CopyOnWriteAfterIteratorHashSet<String> result = new CopyOnWriteAfterIteratorHashSet<>();
        Collections.addAll( result, elements );
        return result;
    }

    private Snapshot<String> snapshot( CopyOnWriteAfterIteratorHashSet<String> set ) {
        return new Snapshot<String>(set.iterator());
    }

    private class Snapshot<E> {
        private final Iterator<E> input;

        private Snapshot( Iterator<E> input )
        {
            this.input = input;
        }

        public Set<E> toSet()
        {
            return IteratorUtil.asSet( input );
        }
    }
}
