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
package org.neo4j.graphdb;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class LuceneLabelScanStoreIT extends LabelScanStoreIT
{

    @Test
    public void scanStoreStartWithoutExistentIndex() throws IOException
    {
        NeoStoreDataSource dataSource = getDataSource();
        LabelScanStore labelScanStore = getLabelScanStore();
        labelScanStore.shutdown();

        File labelScanStoreDirectory = getLabelScanStoreDirectory( dataSource );
        FileUtils.deleteRecursively( labelScanStoreDirectory  );

        labelScanStore.init();
        labelScanStore.start();

        checkLabelScanStoreAccessible( labelScanStore );
    }

    @Test
    public void scanStoreRecreateCorruptedIndexOnStartup() throws IOException
    {
        NeoStoreDataSource dataSource = getDataSource();
        LabelScanStore labelScanStore = getLabelScanStore();

        Node node = createTestNode();
        long[] labels = readNodeLabels( labelScanStore, node );
        assertEquals( "Label scan store see 1 label for node", 1, labels.length );
        labelScanStore.force();
        labelScanStore.shutdown();

        corruptIndex( dataSource );

        labelScanStore.init();
        labelScanStore.start();

        long[] rebuildLabels = readNodeLabels( labelScanStore, node );
        assertArrayEquals( "Store should rebuild corrupted index", labels, rebuildLabels );
    }

    private long[] readNodeLabels( LabelScanStore labelScanStore, Node node )
    {
        try ( LabelScanReader reader = labelScanStore.newReader() )
        {
            return PrimitiveLongCollections.asArray( reader.labelsForNode( node.getId() ) );
        }
    }

    private Node createTestNode()
    {
        Node node = null;
        try (Transaction transaction = dbRule.beginTx())
        {
            node = dbRule.createNode( Label.label( "testLabel" ));
            transaction.success();
        }
        return node;
    }

    private void corruptIndex( NeoStoreDataSource dataSource ) throws IOException
    {
        File labelScanStoreDirectory = getLabelScanStoreDirectory( dataSource );
        Files.walkFileTree( labelScanStoreDirectory.toPath(), new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile( Path file, BasicFileAttributes attrs ) throws IOException
            {
                Files.write( file, ArrayUtils.add(Files.readAllBytes( file ), (byte) 7 ));
                return FileVisitResult.CONTINUE;
            }
        } );
    }

    private File getLabelScanStoreDirectory( NeoStoreDataSource dataSource )
    {
        return LabelScanStoreProvider.getStoreDirectory( dataSource.getStoreDir() );
    }

    private NeoStoreDataSource getDataSource()
    {
        DependencyResolver dependencyResolver = dbRule.getDependencyResolver();
        DataSourceManager dataSourceManager = dependencyResolver.resolveDependency( DataSourceManager.class );
        return dataSourceManager.getDataSource();
    }

    private LabelScanStore getLabelScanStore()
    {
        DependencyResolver dependencyResolver = dbRule.getDependencyResolver();
        return dependencyResolver.resolveDependency( LabelScanStore.class );
    }

    private void checkLabelScanStoreAccessible( LabelScanStore labelScanStore ) throws IOException
    {
        try ( LabelScanWriter labelScanWriter = labelScanStore.newWriter() )
        {
            labelScanWriter.write( NodeLabelUpdate.labelChanges( 1, new long[]{}, new long[]{1} ) );
        }
        try ( LabelScanReader labelScanReader = labelScanStore.newReader() )
        {
            assertEquals( 1, labelScanReader.labelsForNode( 1 ).next() );
        }
    }

}
