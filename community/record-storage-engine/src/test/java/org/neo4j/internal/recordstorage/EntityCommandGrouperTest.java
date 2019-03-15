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

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class EntityCommandGrouperTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    private long nextPropertyId;

    @Test
    public void shouldHandleEmptyList()
    {
        // given
        EntityCommandGrouper<NodeCommand> grouper = new EntityCommandGrouper<>( NodeCommand.class, 8 );

        // when
        boolean hasNext = grouper.nextEntity();

        // then
        assertFalse( hasNext );
    }

    @Test
    public void shouldSeeSingleGroupOfPropertiesWithNode()
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
        grouper.sort();

        // when/then
        assertGroups( grouper, group( nodeId, node, property1, property2 ) );
    }

    @Test
    public void shouldSeeSingleGroupOfPropertiesWithoutNode()
    {
        // given
        EntityCommandGrouper<NodeCommand> grouper = new EntityCommandGrouper<>( NodeCommand.class, 8 );
        long nodeId = 1;
        Command.PropertyCommand property1 = property( nodeId );
        Command.PropertyCommand property2 = property( nodeId );
        grouper.add( property1 );
        grouper.add( property2 );
        grouper.sort();

        // when/then
        assertGroups( grouper, group( nodeId, null, property1, property2 ) );
    }

    @Test
    public void shouldSeeMultipleGroupsSomeOfThemWithNode()
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
        grouper.sort();

        // then
        assertGroups( grouper, groups );
    }

    private NodeCommand node( long nodeId )
    {
        return new NodeCommand( new NodeRecord( nodeId ), new NodeRecord( nodeId ) );
    }

    private void assertGroups( EntityCommandGrouper grouper, Group... groups )
    {
        for ( Group group : groups )
        {
            if ( group.isEmpty() )
            {
                continue;
            }
            assertTrue( grouper.nextEntity() );
            group.assertGroup( grouper );
        }
        assertFalse( grouper.nextEntity() );
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

        void assertGroup( EntityCommandGrouper grouper )
        {
            assertEquals( nodeId, grouper.getCurrentEntity() );
            assertSame( nodeCommand, grouper.getCurrentEntityCommand() );
            Set<Command.PropertyCommand> fromGrouper = new HashSet<>();
            while ( true )
            {
                Command.PropertyCommand property = grouper.nextProperty();
                if ( property == null )
                {
                    break;
                }
                fromGrouper.add( property );
            }
            assertThat( properties, Matchers.equalTo( fromGrouper ) );
        }

        boolean isEmpty()
        {
            return nodeCommand == null && properties.isEmpty();
        }
    }
}
