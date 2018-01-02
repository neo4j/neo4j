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
package org.neo4j.kernel.impl.api.state;

import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Set;

import org.neo4j.kernel.impl.util.diffsets.DiffSets;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class PropertyChangesTest
{
    @Test
    public void shouldListChanges() throws Exception
    {
        // Given
        PropertyChanges changes = new PropertyChanges();
        changes.changeProperty( 1l, 2, "from", "to" );
        changes.addProperty( 1l, 3, "from" );
        changes.removeProperty( 2l, 2, "to" );

        // When & Then
        assertThat( changes.changesForProperty( 2, "to" ), isDiffSets( asSet( 1l ), asSet( 2l ) ) );

        assertThat( changes.changesForProperty( 3, "from" ), isDiffSets( asSet( 1l ), null ) );

        assertThat( changes.changesForProperty( 2, "from" ), isDiffSets( null, asSet( 1l ) ) );
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<? super ReadableDiffSets<Long>> isDiffSets( Set<Long> added, Set<Long> removed )
    {
        return (Matcher) equalTo( new DiffSets<Long>( added, removed ) );
    }
}
