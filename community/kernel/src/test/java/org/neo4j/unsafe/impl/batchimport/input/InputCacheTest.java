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

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.RandomRule;
import org.neo4j.test.Randoms;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

import static java.lang.Math.abs;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_LABELS;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;

public class InputCacheTest
{
    private static final int BATCH_SIZE = 100, BATCHES = 100;

    @Test
    public void shouldCacheAndRetrieveNodes() throws Exception
    {
        // GIVEN
        try ( InputCache cache = new InputCache( fs, dir.directory(), (int) ByteUnit.kibiBytes( 8 ) ) )
        {
            List<InputNode> nodes = new ArrayList<>();
            Randoms random = new Randoms( randomRule.random(), Randoms.DEFAULT );
            try ( Receiver<InputNode[],IOException> cacher = cache.cacheNodes() )
            {
                InputNode[] batch = new InputNode[BATCH_SIZE];
                for ( int b = 0; b < BATCHES; b++ )
                {
                    for ( int i = 0; i < BATCH_SIZE; i++ )
                    {
                        InputNode node = randomNode( random );
                        batch[i] = node;
                        nodes.add( node );
                    }
                    cacher.receive( batch );
                }
            }

            // WHEN/THEN
            try ( InputIterator<InputNode> reader = cache.nodes().iterator() )
            {
                Iterator<InputNode> expected = nodes.iterator();
                while ( expected.hasNext() )
                {
                    assertTrue( reader.hasNext() );
                    InputNode expectedNode = expected.next();
                    InputNode node = reader.next();
                    assertNodesEquals( expectedNode, node );
                }
                assertFalse( reader.hasNext() );
            }
        }
        assertNoFilesLeftBehind();
    }

    @Test
    public void shouldCacheAndRetrieveRelationships() throws Exception
    {
        // GIVEN
        try ( InputCache cache = new InputCache( fs, dir.directory(), (int) ByteUnit.kibiBytes( 8 ) ) )
        {
            List<InputRelationship> relationships = new ArrayList<>();
            Randoms random = new Randoms( randomRule.random(), Randoms.DEFAULT );
            try ( Receiver<InputRelationship[],IOException> cacher = cache.cacheRelationships() )
            {
                InputRelationship[] batch = new InputRelationship[BATCH_SIZE];
                for ( int b = 0; b < BATCHES; b++ )
                {
                    for ( int i = 0; i < BATCH_SIZE; i++ )
                    {
                        InputRelationship relationship = randomRelationship( random );
                        batch[i] = relationship;
                        relationships.add( relationship );
                    }
                    cacher.receive( batch );
                }
            }

            // WHEN/THEN
            try ( InputIterator<InputRelationship> reader = cache.relationships().iterator() )
            {
                Iterator<InputRelationship> expected = relationships.iterator();
                while ( expected.hasNext() )
                {
                    assertTrue( reader.hasNext() );
                    InputRelationship expectedRelationship = expected.next();
                    InputRelationship relationship = reader.next();
                    assertRelationshipsEquals( expectedRelationship, relationship );
                }
                assertFalse( reader.hasNext() );
            }
        }
        assertNoFilesLeftBehind();
    }

    private void assertNoFilesLeftBehind()
    {
        assertEquals( 0, fs.listFiles( dir.directory() ).length );
    }

    private void assertRelationshipsEquals( InputRelationship expectedRelationship, InputRelationship relationship )
    {
        if ( expectedRelationship.hasSpecificId() )
        {
            assertEquals( expectedRelationship.specificId(), relationship.specificId() );
        }
        assertProperties( expectedRelationship, relationship );
        assertEquals( expectedRelationship.startNode(), relationship.startNode() );
        assertEquals( expectedRelationship.startNodeGroup(), relationship.startNodeGroup() );
        assertEquals( expectedRelationship.endNode(), relationship.endNode() );
        assertEquals( expectedRelationship.endNodeGroup(), relationship.endNodeGroup() );
        if ( expectedRelationship.hasTypeId() )
        {
            assertEquals( expectedRelationship.typeId(), relationship.typeId() );
        }
        else
        {
            assertEquals( expectedRelationship.type(), relationship.type() );
        }
    }

    private void assertProperties( InputEntity expected, InputEntity entity )
    {
        if ( expected.hasFirstPropertyId() )
        {
            assertEquals( expected.firstPropertyId(), entity.firstPropertyId() );
        }
        else
        {
            assertArrayEquals( expected.properties(), entity.properties() );
        }
    }

    private InputRelationship randomRelationship( Randoms random )
    {
        if ( random.random().nextFloat() < 0.1f )
        {
            return new InputRelationship( null, 0, 0,
                    NO_PROPERTIES, abs( random.random().nextLong() ),
                    randomGroup( random, 0 ), randomId( random ),
                    randomGroup( random, 1 ), randomId( random ),
                    null, abs( random.random().nextInt( Short.MAX_VALUE ) ) );
        }

        return new InputRelationship( null, 0, 0,
                randomProperties( random ), null,
                randomGroup( random, 0 ), randomId( random ),
                randomGroup( random, 1 ), randomId( random ),
                randomType( random ), null );
    }

    private String randomType( Randoms random )
    {
        if ( previousType == null || random.random().nextFloat() < 0.1f )
        {   // New type
            return previousType = random.among( TOKENS );
        }
        // Keep same as previous
        return previousType;
    }

    private void assertNodesEquals( InputNode expectedNode, InputNode node )
    {
        assertEquals( expectedNode.group(), node.group() );
        assertEquals( expectedNode.id(), node.id() );
        if ( expectedNode.hasFirstPropertyId() )
        {
            assertEquals( expectedNode.firstPropertyId(), node.firstPropertyId() );
        }
        else
        {
            assertArrayEquals( expectedNode.properties(), node.properties() );
        }
        if ( expectedNode.hasLabelField() )
        {
            assertEquals( expectedNode.labelField(), node.labelField() );
        }
        else
        {
            assertEquals( asSet( expectedNode.labels() ), asSet( node.labels() ) );
        }
    }

    private InputNode randomNode( Randoms random )
    {
        if ( random.random().nextFloat() < 0.1f )
        {
            return new InputNode( null, 0, 0, randomId( random ),
                    NO_PROPERTIES, abs( random.random().nextLong() ),
                    NO_LABELS, abs( random.random().nextLong() ) );
        }

        return new InputNode( null, 0, 0,
                randomGroup( random, 0 ), randomId( random ),
                randomProperties( random ), null,
                randomLabels( random ), null );
    }

    private Group randomGroup( Randoms random, int slot )
    {
        if ( random.random().nextFloat() < 0.01f )
        {   // Next group
            return previousGroups[slot] = new Group.Adapter( previousGroups[slot].id()+1, random.string() );
        }
        // Keep same as previous
        return previousGroups[slot];
    }

    private String[] randomLabels( Randoms random )
    {
        if ( previousLabels == null || random.random().nextFloat() < 0.1 )
        {   // Change set of labels
            return previousLabels = random.selection( TOKENS, 1, 5, false );
        }

        // Keep same as previous
        return previousLabels;
    }

    private Object[] randomProperties( Randoms random )
    {
        int length = random.random().nextInt( 10 );
        Object[] properties = new Object[length*2];
        for ( int i = 0; i < properties.length; i++ )
        {
            properties[i++] = random.among( TOKENS );
            properties[i] = random.propertyValue();
        }
        return properties;
    }

    private Object randomId( Randoms random )
    {
        return abs( random.random().nextLong() );
    }

    private static final String[] TOKENS = new String[] { "One", "Two", "Three", "Four", "Five", "Six", "Seven" };
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    public final @Rule TestDirectory dir = TargetDirectory.testDirForTest( getClass() );
    public final @Rule RandomRule randomRule = new RandomRule();

    private String[] previousLabels;
    private final Group[] previousGroups = new Group[] { Group.GLOBAL, Group.GLOBAL };
    private String previousType;
}
