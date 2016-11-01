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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class LuceneLabelScanIndexIT
{

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    private int affectedNodes;

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

    @Parameterized.Parameters( name = "{0}" )
    public static List<Integer> affectedNodes()
    {
        return Arrays.asList( 7, 110, 250, 380, 1400, 2000, 3500, 5800 );
    }

    public LuceneLabelScanIndexIT( int affectedNodes )
    {
        this.affectedNodes = affectedNodes;
    }

    @Test
    public void readFromPartitionedIndex() throws IOException
    {
        try ( LabelScanIndex labelScanIndex = LuceneLabelScanIndexBuilder.create()
                .withFileSystem( fileSystemRule.get() )
                .withIndexIdentifier( "partitionedIndex" + affectedNodes )
                .withIndexRootFolder( testDir.directory( "partitionedIndexFolder" + affectedNodes ) )
                .build() )
        {
            labelScanIndex.open();
            generateLabelChanges( labelScanIndex, affectedNodes );

            try ( LabelScanReader labelScanReader = labelScanIndex.getLabelScanReader() )
            {
                for ( int i = 0; i < affectedNodes; i++ )
                {
                    long[] labels = PrimitiveLongCollections.asArray( labelScanReader.labelsForNode( i ) );
                    assertEquals( "Should have only one label", 1, labels.length );
                    assertEquals( "Label id should be equal to node id", i, labels[0] );
                }

                for ( int i = 0; i < affectedNodes; i++ )
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

    private void generateLabelChanges( LabelScanIndex labelScanIndex, int numberOfUpdates ) throws IOException
    {
        try ( LabelScanWriter scanWriter = labelScanIndex.getLabelScanWriter() )
        {
            generateLabelChanges( scanWriter, numberOfUpdates );
        }
    }
}
