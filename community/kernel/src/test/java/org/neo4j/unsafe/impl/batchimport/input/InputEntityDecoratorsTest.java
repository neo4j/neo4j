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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;

import org.neo4j.helpers.ArrayUtil;
import org.neo4j.unsafe.impl.batchimport.input.csv.Decorator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_LABELS;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.additiveLabels;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.decorators;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.defaultRelationshipType;

public class InputEntityDecoratorsTest
{
    private final InputEntity entity = new InputEntity();

    @Test
    public void shouldProvideDefaultRelationshipType() throws Exception
    {
        // GIVEN
        String defaultType = "TYPE";
        InputEntityVisitor relationship = defaultRelationshipType( defaultType ).apply( entity );

        // WHEN
        relationship( relationship, "source", 1, 0, NO_PROPERTIES, null, "start", "end", null, null );

        // THEN
        assertEquals( defaultType, entity.stringType );
    }

    @Test
    public void shouldNotOverrideAlreadySetRelationshipType() throws Exception
    {
        // GIVEN
        String defaultType = "TYPE";
        InputEntityVisitor relationship = defaultRelationshipType( defaultType ).apply( entity );

        // WHEN
        String customType = "CUSTOM_TYPE";
        relationship( relationship, "source", 1, 0, NO_PROPERTIES, null,
                "start", "end", customType, null );

        // THEN
        assertEquals( customType, entity.stringType );
    }

    @Test
    public void shouldNotOverrideAlreadySetRelationshipTypeId() throws Exception
    {
        // GIVEN
        String defaultType = "TYPE";
        Decorator decorator = defaultRelationshipType( defaultType );
        InputEntityVisitor relationship = decorator.apply( entity );

        // WHEN
        int typeId = 5;
        relationship( relationship, "source", 1, 0, NO_PROPERTIES, null,
                "start", "end", null, typeId );

        // THEN
        assertTrue( entity.hasIntType );
        assertEquals( typeId, entity.intType );
    }

    @Test
    public void shouldAddLabelsToNodeWithoutLabels() throws Exception
    {
        // GIVEN
        String[] toAdd = new String[] {"Add1", "Add2"};
        InputEntityVisitor node = additiveLabels( toAdd ).apply( entity );

        // WHEN
        node( node, "source", 1, 0, "id", NO_PROPERTIES, null, NO_LABELS, null );

        // THEN
        assertArrayEquals( toAdd, entity.labels() );
    }

    @Test
    public void shouldAddMissingLabels() throws Exception
    {
        // GIVEN
        String[] toAdd = new String[] {"Add1", "Add2"};
        InputEntityVisitor node = additiveLabels( toAdd ).apply( entity );

        // WHEN
        String[] nodeLabels = new String[] {"SomeOther"};
        node( node, "source", 1, 0, "id", NO_PROPERTIES, null, nodeLabels, null );

        // THEN
        assertEquals( asSet( ArrayUtil.union( toAdd, nodeLabels ) ), asSet( entity.labels() ) );
    }

    @Test
    public void shouldNotTouchLabelsIfNodeHasLabelFieldSet() throws Exception
    {
        // GIVEN
        String[] toAdd = new String[] {"Add1", "Add2"};
        InputEntityVisitor node = additiveLabels( toAdd ).apply( entity );

        // WHEN
        long labelField = 123L;
        node( node, "source", 1, 0, "id", NO_PROPERTIES, null, null, labelField );

        // THEN
        assertEquals( 0, entity.labels().length );
        assertEquals( labelField, entity.labelField );
    }

    @Test
    public void shouldCramMultipleDecoratorsIntoOne()
    {
        // GIVEN
        Decorator decorator1 = spy( new IdentityDecorator() );
        Decorator decorator2 = spy( new IdentityDecorator() );
        Decorator multi = decorators( decorator1, decorator2 );

        // WHEN
        InputEntityVisitor node = mock( InputEntityVisitor.class );
        multi.apply( node );

        // THEN
        InOrder order = inOrder( decorator1, decorator2 );
        order.verify( decorator1, times( 1 ) ).apply( node );
        order.verify( decorator2, times( 1 ) ).apply( node );
        order.verifyNoMoreInteractions();
    }

    private static void node( InputEntityVisitor entity, String sourceDescription,
            long lineNumber, long position, Object id, Object[] properties, Long propertyId,
            String[] labels, Long labelField ) throws IOException
    {
        applyProperties( entity, properties, propertyId );
        entity.id( id, Group.GLOBAL );
        if ( labelField != null )
        {
            entity.labelField( labelField );
        }
        else
        {
            entity.labels( labels );
        }
        entity.endOfEntity();
    }

    private static void relationship( InputEntityVisitor entity, String sourceDescription, long lineNumber,
            long position, Object[] properties, Long propertyId, Object startNode, Object endNode,
            String type, Integer typeId ) throws IOException
    {
        applyProperties( entity, properties, propertyId );
        entity.startId( startNode, Group.GLOBAL );
        entity.endId( endNode, Group.GLOBAL );
        if ( typeId != null )
        {
            entity.type( typeId );
        }
        else if ( type != null )
        {
            entity.type( type );
        }
        entity.endOfEntity();
    }

    private static void applyProperties( InputEntityVisitor entity, Object[] properties, Long propertyId )
    {
        if ( propertyId != null )
        {
            entity.propertyId( propertyId );
        }
        for ( int i = 0; i < properties.length; i++ )
        {
            entity.property( (String) properties[i++], properties[i] );
        }
    }

    private static class IdentityDecorator implements Decorator
    {
        @Override
        public InputEntityVisitor apply( InputEntityVisitor entity )
        {
            return entity;
        }
    }
}
