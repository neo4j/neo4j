/*
 * Copyright (c) "Neo4j"
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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@ExtendWith( RandomExtension.class )
@EphemeralPageCacheExtension
class RecordNodeCursorIT
{
    private static final int HIGH_LABEL_ID = 0x10000;

    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;
    @Inject
    private RandomSupport random;

    private NeoStores neoStores;
    private NodeStore nodeStore;
    private CachedStoreCursors storeCursors;

    @BeforeEach
    void startNeoStores()
    {
        neoStores = new StoreFactory( RecordDatabaseLayout.ofFlat( directory.homePath() ), Config.defaults(),
                new DefaultIdGeneratorFactory( directory.getFileSystem(), immediate(), "db" ), pageCache, directory.getFileSystem(),
                NullLogProvider.getInstance(), PageCacheTracer.NULL, DatabaseReadOnlyChecker.writable() ).openAllNeoStores( true );
        nodeStore = neoStores.getNodeStore();
        storeCursors = new CachedStoreCursors( neoStores, NULL );
    }

    @AfterEach
    void stopNeoStores()
    {
        storeCursors.close();
        neoStores.close();
    }

    @RepeatedTest( 10 )
    void shouldProperlyReturnHasLabel()
    {
        // given/when
        MutableLongSet labels = LongSets.mutable.empty();
        long nodeId = createNodeWithRandomLabels( labels );

        // then
        try ( RecordNodeCursor nodeCursor = new RecordNodeCursor( nodeStore, neoStores.getRelationshipStore(), neoStores.getRelationshipGroupStore(), null,
                NULL, storeCursors ) )
        {
            nodeCursor.single( nodeId );
            assertThat( nodeCursor.next() ).isTrue();
            for ( int labelId = 0; labelId < HIGH_LABEL_ID; labelId++ )
            {
                boolean fromCursor = nodeCursor.hasLabel( labelId );
                boolean fromSet = labels.contains( labelId );
                assertThat( fromCursor ).as( "Label " + labelId ).isEqualTo( fromSet );
            }
        }
    }

    private long createNodeWithRandomLabels( MutableLongSet labelsSet )
    {
        long[] labels = randomLabels( labelsSet );
        NodeRecord nodeRecord = nodeStore.newRecord();
        nodeRecord.setId( nodeStore.nextId( NULL ) );
        nodeRecord.initialize( true, Record.NO_NEXT_PROPERTY.longValue(), false, Record.NO_NEXT_RELATIONSHIP.longValue(), Record.NO_LABELS_FIELD.longValue() );
        nodeRecord.setCreated();
        NodeLabelsField.parseLabelsField( nodeRecord ).put( labels, nodeStore, nodeStore.getDynamicLabelStore(), NULL, storeCursors, INSTANCE );
        try ( var writeCursor = storeCursors.writeCursor( NODE_CURSOR ) )
        {
            nodeStore.updateRecord( nodeRecord, writeCursor, NULL, storeCursors );
        }
        return nodeRecord.getId();
    }

    private long[] randomLabels( MutableLongSet labelsSet )
    {
        int count = random.nextInt( 0, 100 );
        int highId = random.nextBoolean() ? HIGH_LABEL_ID : count * 3;
        for ( int i = 0; i < count; i++ )
        {
            if ( !labelsSet.add( random.nextInt( highId ) ) )
            {
                i--;
            }
        }
        return labelsSet.toSortedArray();
    }
}
