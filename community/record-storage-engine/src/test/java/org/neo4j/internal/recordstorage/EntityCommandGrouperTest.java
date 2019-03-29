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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
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

    @Test
    void shouldHandleEmptyList()
    {
        // given
        EntityCommandGrouper<NodeCommand> grouper = new EntityCommandGrouper<>( NodeCommand.class, 8 );

        // when
        EntityCommandGrouper.Cursor cursor = grouper.sortAndAccessGroups();
        boolean hasNext = cursor.nextEntity();

        // then
        assertFalse( hasNext );
    }

    @Test
    void shouldSeeSingleGroupOfPropertiesWithNode()
    {
        // given
        EntityCommandGrouper<NodeCommand> grouper = new EntityCommandGrouper<>( NodeCommand.class, 8 );
        long nodeId = 1;
        Command.PropertyCommand property1 = property( nodeId );
        Command.PropertyCommand property2 = property( nodeId );
        NodeCommand node = node( nodeId );
        grouper.add( property1 );
        grouper.add( property2 );
        grouper.add( node ); // <-- deliberately out-of-place
        EntityCommandGrouper.Cursor cursor = grouper.sortAndAccessGroups();

        // when/then
        assertGroups( cursor, group( nodeId, node, property1, property2 ) );
    }

    @Test
    void shouldSeeSingleGroupOfPropertiesWithoutNode()
    {
        // given
        EntityCommandGrouper<NodeCommand> grouper = new EntityCommandGrouper<>( NodeCommand.class, 8 );
        long nodeId = 1;
        Command.PropertyCommand property1 = property( nodeId );
        Command.PropertyCommand property2 = property( nodeId );
        grouper.add( property1 );
        grouper.add( property2 );
        EntityCommandGrouper.Cursor cursor = grouper.sortAndAccessGroups();

        // when/then
        assertGroups( cursor, group( nodeId, null, property1, property2 ) );
    }

    @Test
    void shouldSeeMultipleGroupsSomeOfThemWithNode()
    {
        // given
        EntityCommandGrouper<NodeCommand> grouper = new EntityCommandGrouper<>( NodeCommand.class, 64 );
        Group[] groups = new Group[random.nextInt( 10, 30 )];
        for ( int nodeId = 0; nodeId < groups.length; nodeId++ )
        {
            NodeCommand nodeCommand = random.nextBoolean() ? node( nodeId ) : null;
            groups[nodeId] = new Group( nodeId, nodeCommand );
            if ( nodeCommand != null )
            {
                grouper.add( nodeCommand ); // <-- storage transaction logs are sorted such that node commands comes before property commands
            }
        }
        int totalNumberOfProperties = random.nextInt( 10, 100 );
        for ( int i = 0; i < totalNumberOfProperties; i++ )
        {
            int nodeId = random.nextInt( groups.length );
            Command.PropertyCommand property = property( nodeId );
            groups[nodeId].addProperty( property );
            grouper.add( property );
        }
        // ^^^ OK so we've generated property commands for random nodes in random order, let's sort them
        EntityCommandGrouper.Cursor cursor = grouper.sortAndAccessGroups();

        // then
        assertGroups( cursor, groups );
    }

    @Test
    void shouldWorkOnADifferentSetOfCommandsAfterClear()
    {
        // given
        EntityCommandGrouper<NodeCommand> grouper = new EntityCommandGrouper<>( NodeCommand.class, 16 );
        grouper.add( node( 0 ) );
        grouper.add( node( 1 ) );
        grouper.add( property( 0 ) );
        grouper.add( property( 1 ) );
        grouper.clear();

        // when
        Command.NodeCommand node2 = node( 2 );
        Command.PropertyCommand node2Property = property( 2 );
        Command.NodeCommand node3 = node( 3 );
        grouper.add( node2 );
        grouper.add( node2Property );
        grouper.add( node3 );

        // then
        assertGroups( grouper.sortAndAccessGroups(), group( node2.getKey(), node2, node2Property ), group( node3.getKey(), node3 ) );
    }

    private NodeCommand node( long nodeId )
    {
        return new NodeCommand( new NodeRecord( nodeId ), new NodeRecord( nodeId ) );
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

    private Group group( long nodeId, NodeCommand nodeCommand, Command.PropertyCommand... properties )
    {
        return new Group( nodeId, nodeCommand, properties );
    }

    private Command.PropertyCommand property( long nodeId )
    {
        long propertyId = nextPropertyId++;
        NodeRecord nodeRecord = new NodeRecord( nodeId );
        return new Command.PropertyCommand( new PropertyRecord( propertyId, nodeRecord ), new PropertyRecord( propertyId, nodeRecord ) );
    }

    private static class Group
    {
        private final long nodeId;
        private final NodeCommand nodeCommand;
        private final Set<Command.PropertyCommand> properties = new HashSet<>();

        Group( long nodeId, NodeCommand nodeCommand, Command.PropertyCommand... properties )
        {
            this.nodeId = nodeId;
            this.nodeCommand = nodeCommand;
            this.properties.addAll( Arrays.asList( properties ) );
        }

        void addProperty( Command.PropertyCommand property )
        {
            properties.add( property );
        }

        void assertGroup( EntityCommandGrouper.Cursor cursor )
        {
            assertEquals( nodeId, cursor.currentEntityId() );
            assertSame( nodeCommand, cursor.currentEntityCommand() );
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
            return nodeCommand == null && properties.isEmpty();
        }
    }
}
