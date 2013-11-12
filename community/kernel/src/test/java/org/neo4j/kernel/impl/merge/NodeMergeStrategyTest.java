/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.merge;

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class NodeMergeStrategyTest
{
    private final DefinedProperty property1 = Property.intProperty( 10, 200 );
    private final DefinedProperty property2 = Property.intProperty( 11, 100 );

    @Test
    public void shouldSortByTypeAtFirst() throws Exception
    {
        // given
        NodeMergeStrategyBuilder builder = new NodeMergeStrategyBuilder();
        builder
            .addUsingIndex( 1, property1, null )
            .addUsingLabel( 1, property1 )
            .addUsingUniqueIndex( 1, property1, null );

        // when
        List<NodeMergeStrategy> list = builder.buildStrategies();

        // then
        Iterator<NodeMergeStrategy> iterator = list.iterator();
        assertEquals( NodeMergeStrategy.Type.UNIQUE_INDEX, iterator.next().type );
        assertEquals( NodeMergeStrategy.Type.REGULAR_INDEX, iterator.next().type );
        assertEquals( NodeMergeStrategy.Type.LABEL_SCAN, iterator.next().type );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldSortByLabelAtSecond() throws Exception
    {
        // given
        NodeMergeStrategyBuilder builder = new NodeMergeStrategyBuilder();
        builder
            .addUsingLabel( 2, property1 )
            .addUsingLabel( 1, property2 );

        // when
        List<NodeMergeStrategy> list = builder.buildStrategies();

        // then
        Iterator<NodeMergeStrategy> iterator = list.iterator();
        assertEquals( 1, iterator.next().labelId );
        assertEquals( 2, iterator.next().labelId );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldSortByPropertyAtThird() throws Exception
    {
        // given
        NodeMergeStrategyBuilder builder = new NodeMergeStrategyBuilder();
        builder
            .addUsingLabel( 1, property2 )
            .addUsingLabel( 1, property1 );

        // when
        List<NodeMergeStrategy> list = builder.buildStrategies();

        // then
        Iterator<NodeMergeStrategy> iterator = list.iterator();
        assertEquals( property1, iterator.next().property );
        assertEquals( property2, iterator.next().property );
        assertFalse( iterator.hasNext() );
    }
}
