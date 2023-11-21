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
package org.neo4j.kernel.impl.index.schema;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.countUniqueValues;
import static org.neo4j.values.storable.RandomValues.typesOfGroup;
import static org.neo4j.values.storable.ValueGroup.NUMBER;

public class FullScanNonUniqueIndexSamplerTest extends IndexTestUtil<BtreeKey,NullValue,IndexLayout<BtreeKey>>
{
    private static final IndexSpecificSpaceFillingCurveSettings specificSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );

    private static final IndexDescriptor index = forSchema( forLabel( 42, 666 ) ).withName( "index" ).materialise( 0 );

    NativeValueIndexUtility<BtreeKey> valueUtil;
    ValueCreatorUtil<BtreeKey> valueCreatorUtil;

    @BeforeEach
    void setupValueCreator()
    {
        valueCreatorUtil = createValueCreatorUtil();
        valueUtil = new NativeValueIndexUtility<>( valueCreatorUtil, layout );
    }

    @Test
    void shouldIncludeAllValuesInTree() throws Exception
    {
        // GIVEN
        Value[] values = generateNumberValues();
        buildTree( values );

        // WHEN
        IndexSample sample;
        try ( GBPTree<BtreeKey,NullValue> gbpTree = getTree() )
        {
            FullScanNonUniqueIndexSampler<BtreeKey> sampler = new FullScanNonUniqueIndexSampler<>( gbpTree, layout );
            sample = sampler.sample( NULL, new AtomicBoolean() );
        }

        // THEN
        assertEquals( values.length, sample.sampleSize() );
        assertEquals( countUniqueValues( values ), sample.uniqueValues() );
        assertEquals( values.length, sample.indexSize() );
    }

    @Test
    void shouldStopIfFlagged() throws Exception
    {
        // GIVEN
        Value[] values = generateNumberValues();
        buildTree( values );

        // WHEN
        IndexSample sample;
        try ( GBPTree<BtreeKey,NullValue> gbpTree = getTree() )
        {
            FullScanNonUniqueIndexSampler<BtreeKey> sampler = new FullScanNonUniqueIndexSampler<>( gbpTree, layout );
            sample = sampler.sample( NULL, new AtomicBoolean( true ) );
        }

        // THEN
        assertEquals( 0, sample.sampleSize() );
        assertEquals( 0, sample.uniqueValues() );
        assertEquals( 0, sample.indexSize() );
    }

    @Test
    void tracePageCacheAccessOnSampling() throws IOException
    {
        Value[] values = generateNumberValues();
        buildTree( values );

        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "testTracer" ) );

        assertZeroCursor( cursorContext );

        try ( GBPTree<BtreeKey,NullValue> gbpTree = getTree() )
        {
            FullScanNonUniqueIndexSampler<BtreeKey> sampler = new FullScanNonUniqueIndexSampler<>( gbpTree, layout );
            sampler.sample( cursorContext, new AtomicBoolean() );
        }

        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat( cursorTracer.pins() ).isEqualTo( 1 );
        assertThat( cursorTracer.unpins() ).isEqualTo( 1 );
        assertThat( cursorTracer.faults() ).isEqualTo( 1 );
    }

    private static void assertZeroCursor( CursorContext cursorContext )
    {
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat( cursorTracer.pins() ).isZero();
        assertThat( cursorTracer.unpins() ).isZero();
        assertThat( cursorTracer.faults() ).isZero();
    }

    private Value[] generateNumberValues()
    {
        ValueType[] numberTypes = RandomValues.including( t -> t.valueGroup == NUMBER );
        int size = 20;
        Value[] result = new NumberValue[size];
        for ( int i = 0; i < size; i++ )
        {
            result[i] = random.randomValues().nextValueOfTypes( numberTypes );
        }
        return result;
    }

    private void buildTree( Value[] values ) throws IOException
    {
        try ( GBPTree<BtreeKey,NullValue> gbpTree = getTree() )
        {
            try ( Writer<BtreeKey,NullValue> writer = gbpTree.writer( NULL ) )
            {
                BtreeKey key = layout.newKey();
                long nodeId = 0;
                for ( Value number : values )
                {
                    key.initialize( nodeId );
                    key.initFromValue( 0, number, NEUTRAL );
                    writer.put( key, NullValue.INSTANCE );
                    nodeId++;
                }
            }
            gbpTree.checkpoint( NULL );
        }
    }

    private static ValueCreatorUtil<BtreeKey> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( index, typesOfGroup( NUMBER ), FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexFiles createIndexFiles( FileSystemAbstraction fs, TestDirectory directory, IndexDescriptor indexDescriptor )
    {
        IndexDirectoryStructure indexDirectoryStructure =
                directoriesByProvider( directory.directory( "root" ) ).forProvider( indexDescriptor.getIndexProvider() );
        return new IndexFiles.Directory( fs, indexDirectoryStructure, indexDescriptor.getId() );
    }

    @Override
    IndexDescriptor indexDescriptor()
    {
        return index;
    }

    @Override
    IndexLayout<BtreeKey> layout()
    {
        return new GenericLayout( 1,specificSettings );
    }
}
