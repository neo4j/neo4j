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
package org.neo4j.consistency.checking.full;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.consistency.checking.full.ConsistencyFlags.DEFAULT;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.test.mockito.mock.Property.property;
import static org.neo4j.test.mockito.mock.Property.set;

@TestDirectoryExtension
class SpecialisedIndexFullCheckTest
{
    private static final String PROP1 = "key1";
    private static final String PROP2 = "key2";
    private static final String INDEXED_VALUE = "some text";
    private static final String ANOTHER_INDEXED_VALUE = "another piece of text";
    private static final int NOT_INDEXED_VALUE = 123;
    private final Map<Setting<?>,Object> settings = new HashMap<>();
    private GraphStoreFixture fixture;

    @Inject
    private TestDirectory testDirectory;

    @BeforeEach
    protected void setUp()
    {
        fixture = createFixture();
    }

    @AfterEach
    void tearDown()
    {
        fixture.close();
    }

    @ParameterizedTest
    @EnumSource( IndexSize.class )
    void shouldReportNodesThatAreIndexedWhenTheyShouldNotBe( IndexSize indexSize ) throws Exception
    {
        indexSize.createAdditionalData( fixture );

        // given
        long newNode = createOneNode();

        Iterator<IndexDescriptor> indexDescriptorIterator = getValueIndexDescriptors();
        while ( indexDescriptorIterator.hasNext() )
        {
            IndexDescriptor indexDescriptor = indexDescriptorIterator.next();
            IndexAccessor accessor = fixture.indexAccessorLookup().apply( indexDescriptor );
            try ( IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE, NULL ) )
            {
                updater.process( IndexEntryUpdate.add( newNode, indexDescriptor, values( indexDescriptor ) ) );
            }
        }

        // when
        ConsistencySummaryStatistics stats = check();

        assertFalse( stats.isConsistent() );
        RecordType recordType = indexSize == IndexSize.SMALL_INDEX ? RecordType.INDEX : RecordType.NODE;
        assertThat( stats.getInconsistencyCountForRecordType( recordType ) ).isEqualTo( 2 );
    }

    Value[] values( IndexDescriptor indexRule )
    {
        switch ( indexRule.schema().getPropertyIds().length )
        {
        case 1:
            return Iterators.array( Values.of( INDEXED_VALUE ) );
        case 2:
            return Iterators.array( Values.of( INDEXED_VALUE ), Values.of( ANOTHER_INDEXED_VALUE ) );
        default:
            throw new UnsupportedOperationException();
        }
    }

    private Iterator<IndexDescriptor> getValueIndexDescriptors()
    {
        return Iterators.filter( descriptor -> !descriptor.isTokenIndex(), fixture.getIndexDescriptors() );
    }

    private ConsistencySummaryStatistics check() throws ConsistencyCheckIncompleteException
    {
        // the database must not be running during the check because of Lucene-based indexes
        // Lucene files are locked when the DB is running
        fixture.close();

        var config = Config.newBuilder().set( GraphDatabaseSettings.neo4j_home, testDirectory.homePath() ).build();

        ConsistencyCheckService checkService = new ConsistencyCheckService();
        return checkService.runFullConsistencyCheck( Neo4jLayout.of( config ).databaseLayout( "neo4j" ),
                config, ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), false, DEFAULT ).summary();
    }

    private GraphStoreFixture createFixture()
    {
        return new GraphStoreFixture( StringUtils.EMPTY, testDirectory )
        {
            @Override
            protected void generateInitialData( GraphDatabaseService db )
            {
                try ( var tx = db.beginTx() )
                {
                    tx.schema().indexFor( label( "Label1" ) ).on( PROP1 ).withIndexType( IndexType.FULLTEXT ).create();
                    tx.schema().indexFor( label( "Label1" ) ).on( PROP2 ).withIndexType( IndexType.FULLTEXT ).create();

                    tx.commit();
                }
                try ( var tx = db.beginTx() )
                {
                    tx.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES );
                }

                // Create initial data
                try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
                {
                    set( tx.createNode( label( "Label1" ) ), property( PROP1, INDEXED_VALUE ) );
                    set( tx.createNode( label( "Label1" ) ), property( PROP1, INDEXED_VALUE ), property( PROP2, ANOTHER_INDEXED_VALUE ) );
                    set( tx.createNode( label( "Label1" ) ), property( PROP1, NOT_INDEXED_VALUE ) );
                    set( tx.createNode( label( "AnotherLabel" ) ), property( PROP1, INDEXED_VALUE ) );
                    set( tx.createNode( label( "Label1" ) ), property( "anotherProperty", INDEXED_VALUE ) );
                    tx.createNode();

                    tx.commit();
                }
            }

            @Override
            protected Map<Setting<?>,Object> getConfig()
            {
                return settings;
            }
        };
    }

    protected long createOneNode()
    {
        final AtomicLong id = new AtomicLong();
        fixture.apply( tx ->
        {
            id.set( tx.createNode().getId() );
        } );
        return id.get();
    }

    /**
     * Indexes are consistency checked in different ways depending on their size. This can be used to make the indexes created in the setup appear large or
     * small.
     */
    private enum IndexSize
    {
        SMALL_INDEX
                {
                    @Override
                    public void createAdditionalData( GraphStoreFixture fixture )
                    {
                        fixture.apply( tx ->
                        {
                            // Create more nodes so our indexes will be considered to be small indexes (less than 5% of nodes in index).
                            for ( int i = 0; i < 80; i++ )
                            {
                                tx.createNode();
                            }
                        } );
                    }
                },
        LARGE_INDEX
                {
                    @Override
                    public void createAdditionalData( GraphStoreFixture fixture )
                    {
                    }
                };

        public abstract void createAdditionalData( GraphStoreFixture fixture );
    }
}
