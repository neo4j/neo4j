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

import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.impl.util.collection.OnHeapCollectionsFactory;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveLongDiffSets;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptySet;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.setOf;

public class PropertyChangesTest
{
    @Test
    public void shouldListChanges()
    {
        // Given
        PropertyChanges changes = new PropertyChanges( OnHeapCollectionsFactory.INSTANCE );
        changes.changeProperty( 1L, 2, "from", "to" );
        changes.addProperty( 1L, 3, "from" );
        changes.removeProperty( 2L, 2, "to" );

        // When & Then
        assertThat( changes.changesForProperty( 2, "to" ), isDiffSets( setOf( 1L ), setOf( 2L ) ) );

        assertThat( changes.changesForProperty( 3, "from" ), isDiffSets( setOf( 1L ), emptySet() ) );

        assertThat( changes.changesForProperty( 2, "from" ), isDiffSets( emptySet(), setOf( 1L ) ) );
    }

    @SuppressWarnings( "unchecked" )
    private Matcher<? super PrimitiveLongReadableDiffSets> isDiffSets( PrimitiveLongSet added, PrimitiveLongSet removed )
    {
        return (Matcher) equalTo( new PrimitiveLongDiffSets( added, removed, OnHeapCollectionsFactory.INSTANCE ) );
    }
}
