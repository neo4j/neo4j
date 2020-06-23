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
package org.neo4j.graphdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.TokenScanReader;
import org.neo4j.internal.index.label.TokenScanStoreTest;
import org.neo4j.internal.index.label.TokenScanWriter;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongCollections.closingAsArray;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@DbmsExtension
@ExtendWith( RandomExtension.class )
class LabelScanStoreStartupIT
{
    private static final Label LABEL = Label.label( "testLabel" );
    @Inject
    private GraphDatabaseAPI databaseAPI;
    @Inject
    private RandomRule random;
    @Inject
    private LabelScanStore labelScanStore;
    @Inject
    private RecoveryCleanupWorkCollector workCollector;

    private int labelId;

    @Test
    void scanStoreStartWithoutExistentIndex() throws Throwable
    {
        labelScanStore.shutdown();
        workCollector.shutdown();

        deleteLabelScanStoreFiles( databaseAPI.databaseLayout() );

        workCollector.init();
        labelScanStore.init();
        workCollector.start();
        labelScanStore.start();

        checkLabelScanStoreAccessible( labelScanStore );
    }

    @Test
    void scanStoreRecreateCorruptedIndexOnStartup() throws Throwable
    {
        createTestNode();
        long[] labels = readNodesForLabel( labelScanStore );
        assertEquals( 1, labels.length, "Label scan store see 1 label for node" );
        labelScanStore.force( IOLimiter.UNLIMITED, NULL );
        labelScanStore.shutdown();
        workCollector.shutdown();

        corruptLabelScanStoreFiles( databaseAPI.databaseLayout() );

        workCollector.init();
        labelScanStore.init();
        workCollector.start();
        labelScanStore.start();

        long[] rebuildLabels = readNodesForLabel( labelScanStore );
        assertArrayEquals( labels, rebuildLabels, "Store should rebuild corrupted index" );
    }

    private long[] readNodesForLabel( LabelScanStore labelScanStore )
    {
        return closingAsArray( labelScanStore.newReader().entitiesWithToken( labelId, NULL ) );
    }

    private void createTestNode()
    {
        try ( Transaction transaction = databaseAPI.beginTx() )
        {
            var node = transaction.createNode( LABEL );
            KernelTransaction ktx = ((InternalTransaction) transaction).kernelTransaction();
            labelId = ktx.tokenRead().nodeLabel( LABEL.name() );
            transaction.commit();
        }
    }

    private void scrambleFile( File file ) throws IOException
    {
        TokenScanStoreTest.scrambleFile( random.random(), file );
    }

    private static File storeFile( DatabaseLayout databaseLayout )
    {
        return databaseLayout.labelScanStore().toFile();
    }

    private void corruptLabelScanStoreFiles( DatabaseLayout databaseLayout ) throws IOException
    {
        scrambleFile( storeFile( databaseLayout ) );
    }

    private static void deleteLabelScanStoreFiles( DatabaseLayout databaseLayout )
    {
        assertTrue( storeFile( databaseLayout ).delete() );
    }

    private static void checkLabelScanStoreAccessible( LabelScanStore labelScanStore ) throws IOException
    {
        int labelId = 1;
        try ( TokenScanWriter labelScanWriter = labelScanStore.newWriter( NULL ) )
        {
            labelScanWriter.write( EntityTokenUpdate.tokenChanges( 1, new long[]{}, new long[]{labelId} ) );
        }
        TokenScanReader labelScanReader = labelScanStore.newReader();
        try ( PrimitiveLongResourceIterator iterator = labelScanReader.entitiesWithToken( labelId, NULL ) )
        {
            assertEquals( 1, iterator.next() );
        }
    }
}
