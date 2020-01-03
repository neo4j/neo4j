/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.api.index;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.BaseCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( RandomExtension.class )
class EntityCommandGrouperTest
{
    @Inject
    private RandomRule random;

    private long nextPropertyId;

    @ParameterizedTest
    @EnumSource( Factory.class )
    void shouldHandleEmptyList( Factory factory )
    {
        // given
        EntityCommandGrouper grouper = new EntityCommandGrouper<>( factory.command( 0 ).getClass(), 8 );

        // when
        EntityCommandGrouper.Cursor cursor = grouper.sortAndAccessGroups();
        boolean hasNext = cursor.nextEntity();

        // then
        assertFalse( hasNext );
    }

    @ParameterizedTest
    @EnumSource( Factory.class )
    void shouldSeeSingleGroupOfPropertiesWithEntity( Factory factory )
    {
        // given
        EntityCommandGrouper grouper = new EntityCommandGrouper<>( factory.command( 0 ).getClass(), 8 );
        long entityId = 1;
        BaseCommand<? extends PrimitiveRecord> entity = factory.command( entityId );
        Command.PropertyCommand property1 = property( entity.getAfter() );
        Command.PropertyCommand property2 = property( entity.getAfter() );
        grouper.add( property1 );
        grouper.add( property2 );
        grouper.add( entity ); // <-- deliberately out-of-place
        EntityCommandGrouper.Cursor cursor = grouper.sortAndAccessGroups();

        // when/then
        assertGroups( cursor, group( entityId, entity, property1, property2 ) );
    }

    @ParameterizedTest
    @EnumSource( Factory.class )
    void shouldSeeSingleGroupOfPropertiesWithoutEntity( Factory factory )
    {
        // given
        EntityCommandGrouper grouper = new EntityCommandGrouper<>( factory.command( 0 ).getClass(), 8 );
        long entityId = 1;
        BaseCommand<? extends PrimitiveRecord> entity = factory.command( entityId );
        Command.PropertyCommand property1 = property( entity.getAfter() );
        Command.PropertyCommand property2 = property( entity.getAfter() );
        // intentionally DO NOT add the entity command
        grouper.add( property1 );
        grouper.add( property2 );
        EntityCommandGrouper.Cursor cursor = grouper.sortAndAccessGroups();

        // when/then
        assertGroups( cursor, group( entityId, null, property1, property2 ) );
    }

    @ParameterizedTest
    @EnumSource( Factory.class )
    void shouldSeeMultipleGroupsSomeOfThemWithEntity( Factory factory )
    {
        // given
        EntityCommandGrouper grouper = new EntityCommandGrouper<>( factory.command( 0 ).getClass(), 64 );
        Group[] groups = new Group[random.nextInt( 10, 30 )];
        for ( int entityId = 0; entityId < groups.length; entityId++ )
        {
            BaseCommand entityCommand = random.nextBoolean() ? factory.command( entityId ) : null;
            groups[entityId] = new Group( entityId, entityCommand );
            if ( entityCommand != null )
            {
                grouper.add( entityCommand ); // <-- storage transaction logs are sorted such that entity commands comes before property commands
            }
        }
        int totalNumberOfProperties = random.nextInt( 10, 100 );
        for ( int i = 0; i < totalNumberOfProperties; i++ )
        {
            int entityId = random.nextInt( groups.length );
            Command.PropertyCommand property = property( factory.command( entityId ).getAfter() );
            groups[entityId].addProperty( property );
            grouper.add( property );
        }
        // ^^^ OK so we've generated property commands for random entities in random order, let's sort them
        EntityCommandGrouper.Cursor cursor = grouper.sortAndAccessGroups();

        // then
        assertGroups( cursor, groups );
    }

    @ParameterizedTest
    @EnumSource( Factory.class )
    void shouldWorkOnADifferentSetOfCommandsAfterClear( Factory factory )
    {
        // given
        EntityCommandGrouper grouper = new EntityCommandGrouper<>( factory.command( 0 ).getClass(), 16 );
        BaseCommand<? extends PrimitiveRecord> entity0 = factory.command( 0 );
        BaseCommand<? extends PrimitiveRecord> entity1 = factory.command( 1 );
        grouper.add( entity0 );
        grouper.add( entity1 );
        grouper.add( property( entity0.getAfter() ) );
        grouper.add( property( entity1.getAfter() ) );
        grouper.clear();

        // when
        BaseCommand<? extends PrimitiveRecord> entity2 = factory.command( 2 );
        Command.PropertyCommand entityProperty = property( entity2.getAfter() );
        BaseCommand<? extends PrimitiveRecord> entity3 = factory.command( 3 );
        grouper.add( entity2 );
        grouper.add( entityProperty );
        grouper.add( entity3 );

        // then
        assertGroups( grouper.sortAndAccessGroups(), group( entity2.getKey(), entity2, entityProperty ), group( entity3.getKey(), entity3 ) );
    }

    private void assertGroups( EntityCommandGrouper.Cursor cursor, Group... groups )
    {
        for ( Group group : groups )
        {
            if ( group.isEmpty() )
            {
                continue;
            }
            assertTrue( cursor.nextEntity() );
            group.assertGroup( cursor );
        }
        assertFalse( cursor.nextEntity() );
    }

    private Group group( long entityId, BaseCommand<? extends PrimitiveRecord> entityCommand, Command.PropertyCommand... properties )
    {
        return new Group( entityId, entityCommand, properties );
    }

    private Command.PropertyCommand property( PrimitiveRecord owner )
    {
        long propertyId = nextPropertyId++;
        return new Command.PropertyCommand( new PropertyRecord( propertyId, owner ), new PropertyRecord( propertyId, owner ) );
    }

    private static class Group
    {
        private final long entityId;
        private final Command entityCommand;
        private final Set<Command.PropertyCommand> properties = new HashSet<>();

        Group( long entityId, Command entityCommand, Command.PropertyCommand... properties )
        {
            this.entityId = entityId;
            this.entityCommand = entityCommand;
            this.properties.addAll( Arrays.asList( properties ) );
        }

        void addProperty( Command.PropertyCommand property )
        {
            properties.add( property );
        }

        void assertGroup( EntityCommandGrouper.Cursor cursor )
        {
            assertEquals( entityId, cursor.currentEntityId() );
            assertSame( entityCommand, cursor.currentEntityCommand() );
            Set<Command.PropertyCommand> fromGrouper = new HashSet<>();
            while ( true )
            {
                Command.PropertyCommand property = cursor.nextProperty();
                if ( property == null )
                {
                    break;
                }
                fromGrouper.add( property );
            }
            assertEquals( fromGrouper, properties );
        }

        boolean isEmpty()
        {
            return entityCommand == null && properties.isEmpty();
        }
    }

    private enum Factory
    {
        NODE
                {
                    @Override
                    NodeCommand command( long value )
                    {
                        return new NodeCommand( new NodeRecord( value ), new NodeRecord( value ) );
                    }
                },
        RELATIONSHIP
                {
                    @Override
                    RelationshipCommand command( long value )
                    {
                        return new RelationshipCommand( new RelationshipRecord( value ), new RelationshipRecord( value ) );
                    }
                };

        abstract BaseCommand<? extends PrimitiveRecord> command( long id );
    }
}
