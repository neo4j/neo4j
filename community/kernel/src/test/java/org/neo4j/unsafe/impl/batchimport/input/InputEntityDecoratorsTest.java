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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.function.Function;
import org.neo4j.helpers.ArrayUtil;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class InputEntityDecoratorsTest
{
    @Test
    public void shouldProvideDefaultRelationshipType() throws Exception
    {
        // GIVEN
        String defaultType = "TYPE";
        Function<InputRelationship,InputRelationship> decorator =
                InputEntityDecorators.defaultRelationshipType( defaultType );

        // WHEN
        InputRelationship relationship = new InputRelationship( "source", 1, 0, InputEntity.NO_PROPERTIES, null,
                "start", "end", null, null );
        relationship = decorator.apply( relationship );

        // THEN
        assertEquals( defaultType, relationship.type() );
    }

    @Test
    public void shouldNotOverrideAlreadySetRelationshipType() throws Exception
    {
        // GIVEN
        String defaultType = "TYPE";
        Function<InputRelationship,InputRelationship> decorator =
                InputEntityDecorators.defaultRelationshipType( defaultType );

        // WHEN
        String customType = "CUSTOM_TYPE";
        InputRelationship relationship = new InputRelationship( "source", 1, 0, InputEntity.NO_PROPERTIES, null,
                "start", "end", customType, null );
        relationship = decorator.apply( relationship );

        // THEN
        assertEquals( customType, relationship.type() );
    }

    @Test
    public void shouldNotOverrideAlreadySetRelationshipTypeId() throws Exception
    {
        // GIVEN
        String defaultType = "TYPE";
        Function<InputRelationship,InputRelationship> decorator =
                InputEntityDecorators.defaultRelationshipType( defaultType );

        // WHEN
        Integer typeId = 5;
        InputRelationship relationship = new InputRelationship( "source", 1, 0, InputEntity.NO_PROPERTIES, null,
                "start", "end", null, typeId );
        relationship = decorator.apply( relationship );

        // THEN
        assertEquals( null, relationship.type() );
        assertEquals( typeId.intValue(), relationship.typeId() );
    }

    @Test
    public void shouldAddLabelsToNodeWithoutLabels() throws Exception
    {
        // GIVEN
        String[] toAdd = new String[] {"Add1", "Add2"};
        Function<InputNode,InputNode> decorator = InputEntityDecorators.additiveLabels( toAdd );

        // WHEN
        InputNode node = new InputNode( "source", 1, 0, "id", InputEntity.NO_PROPERTIES, null, null, null );
        node = decorator.apply( node );

        // THEN
        assertArrayEquals( toAdd, node.labels() );
    }

    @Test
    public void shouldAddMissingLabels() throws Exception
    {
        // GIVEN
        String[] toAdd = new String[] {"Add1", "Add2"};
        Function<InputNode,InputNode> decorator = InputEntityDecorators.additiveLabels( toAdd );

        // WHEN
        String[] nodeLabels = new String[] {"SomeOther"};
        InputNode node = new InputNode( "source", 1, 0, "id", InputEntity.NO_PROPERTIES, null, nodeLabels, null );
        node = decorator.apply( node );

        // THEN
        assertEquals( asSet( ArrayUtil.union( toAdd, nodeLabels ) ), asSet( node.labels() ) );
    }

    @Test
    public void shouldNotTouchLabelsIfNodeHasLabelFieldSet() throws Exception
    {
        // GIVEN
        String[] toAdd = new String[] {"Add1", "Add2"};
        Function<InputNode,InputNode> decorator = InputEntityDecorators.additiveLabels( toAdd );

        // WHEN
        long labelField = 123L;
        InputNode node = new InputNode( "source", 1, 0, "id", InputEntity.NO_PROPERTIES, null, null, labelField );
        node = decorator.apply( node );

        // THEN
        assertNull( node.labels() );
        assertEquals( labelField, node.labelField().longValue() );
    }

    @Test
    public void shouldCramMultipleDecoratorsIntoOne() throws Exception
    {
        // GIVEN
        Function<InputNode,InputNode> decorator1 = spy( new IdentityDecorator() );
        Function<InputNode,InputNode> decorator2 = spy( new IdentityDecorator() );
        Function<InputNode,InputNode> multi = InputEntityDecorators.decorators( decorator1, decorator2 );

        // WHEN
        InputNode node = mock( InputNode.class );
        multi.apply( node );

        // THEN
        InOrder order = inOrder( decorator1, decorator2 );
        order.verify( decorator1, times( 1 ) ).apply( node );
        order.verify( decorator2, times( 1 ) ).apply( node );
        order.verifyNoMoreInteractions();
    }

    private static class IdentityDecorator implements Function<InputNode,InputNode>
    {
        @Override
        public InputNode apply( InputNode from ) throws RuntimeException
        {
            return from;
        }
    }
}
