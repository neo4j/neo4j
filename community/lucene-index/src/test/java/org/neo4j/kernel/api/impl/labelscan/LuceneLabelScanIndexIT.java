/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.labelscan;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;

public class LuceneLabelScanIndexIT
{

    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Before
    public void before() throws Exception
    {
        System.setProperty( "labelScanStore.maxPartitionSize", "10" );
    }

    @After
    public void after() throws IOException
    {
        System.setProperty( "labelScanStore.maxPartitionSize", "" );
    }

    @Test
    public void createPartitionedLabelScanIndex() throws IOException
    {
        try ( LuceneLabelScanIndex labelScanIndex = LuceneLabelScanIndexBuilder.create()
                .withIndexIdentifier( "partitionedIndex" )
                .withIndexRootFolder( testDir.directory( "partitionedIndexFolder" ) )
                .build() )
        {
            labelScanIndex.open();

            generateLabelChanges( labelScanIndex, 1500 );

            assertEquals( 5, labelScanIndex.getPartitions().size() );
        }
    }

    @Test
    public void readFromSinglePartitionedIndex() throws IOException
    {
        try ( LuceneLabelScanIndex labelScanIndex = LuceneLabelScanIndexBuilder.create()
                .withIndexIdentifier( "partitionedIndex" )
                .withIndexRootFolder( testDir.directory( "partitionedIndexFolder" ) )
                .build() )
        {
            labelScanIndex.open();
            int numberOfUpdates = 100;
            generateLabelChanges( labelScanIndex, numberOfUpdates );

            try ( LabelScanReader labelScanReader = labelScanIndex.getLabelScanReader() )
            {
                for ( int i = 0; i < numberOfUpdates; i++ )
                {
                    List<Long> labels = Iterables.toList( labelScanReader.labelsForNode( i ) );
                    assertEquals( "Should have only one label", 1, labels.size() );
                    assertEquals( "Label id should be equal to node id", i, labels.get( 0 ).intValue() );
                }

                for ( int i = 0; i < numberOfUpdates; i++ )
                {
                    long[] nodes = PrimitiveLongCollections.asArray( labelScanReader.nodesWithLabel( i ) );
                    assertEquals( "Should have only one node for each label", 1, nodes.length );
                    assertEquals( "Label id should be equal to node id", i, nodes[0] );
                }
            }
        }
    }

    private void generateLabelChanges( LabelScanWriter scanWriter, int affectedNodes ) throws IOException
    {
        for ( int i = 0; i < affectedNodes; i++ )
        {
            scanWriter.write( NodeLabelUpdate.labelChanges( i, new long[]{}, new long[]{i} ) );
        }
    }

    private void generateLabelChanges( LuceneLabelScanIndex labelScanIndex, int numberOfUpdates ) throws IOException
    {
        ReentrantLock lock = new ReentrantLock();
        lock.lock();
        try ( LabelScanWriter scanWriter = labelScanIndex.getLabelScanWriter( lock ) )
        {
            generateLabelChanges( scanWriter, numberOfUpdates );
        }
    }
}
