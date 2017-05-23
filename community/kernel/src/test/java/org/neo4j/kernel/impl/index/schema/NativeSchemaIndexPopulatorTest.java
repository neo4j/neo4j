/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.codec.Charsets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import org.neo4j.cursor.RawCursor;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Header;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.SchemaNumberValue.DOUBLE;
import static org.neo4j.kernel.impl.index.schema.SchemaNumberValue.FLOAT;
import static org.neo4j.kernel.impl.index.schema.SchemaNumberValue.LONG;
import static org.neo4j.test.rule.PageCacheRule.config;

public abstract class NativeSchemaIndexPopulatorTest<KEY extends SchemaNumberKey,VALUE extends SchemaNumberValue>
{
    static final int LARGE_AMOUNT_OF_UPDATES = 10_000;
    private static final IndexDescriptor indexDescriptor = IndexDescriptorFactory.forLabel( 42, 666 );
    static final PropertyAccessor null_property_accessor = ( nodeId, propKeyId ) -> null;

    private final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withAccessChecks( true ) );
    protected final RandomRule random = new RandomRule();
    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    private Layout<KEY,VALUE> layout;
    File indexFile;
    PageCache pageCache;
    NativeSchemaIndexPopulator<KEY,VALUE> populator;

    @Before
    public void setup()
    {
        layout = createLayout();
        indexFile = directory.file( "index" );
        pageCache = pageCacheRule.getPageCache( fs );
        IndexSamplingConfig samplingConfig = new IndexSamplingConfig( Config.embeddedDefaults() );
        populator = createPopulator( pageCache, indexFile, layout, samplingConfig );
    }

    abstract Layout<KEY,VALUE> createLayout();

    abstract NativeSchemaIndexPopulator<KEY,VALUE> createPopulator( PageCache pageCache, File indexFile,
            Layout<KEY,VALUE> layout, IndexSamplingConfig samplingConfig );

    @Test
    public void createShouldCreateFile() throws Exception
    {
        // given
        assertFalse( fs.fileExists( indexFile ) );

        // when
        populator.create();

        // then
        assertTrue( fs.fileExists( indexFile ) );
        populator.close( true );
    }

    @Test
    public void createShouldClearExistingFile() throws Exception
    {
        // given
        byte[] someBytes = fileWithContent();

        // when
        populator.create();

        // then
        try ( StoreChannel r = fs.open( indexFile, "r" ) )
        {
            byte[] firstBytes = new byte[someBytes.length];
            r.read( ByteBuffer.wrap( firstBytes ) );
            assertNotEquals( "Expected previous file content to have been cleared but was still there",
                    someBytes, firstBytes );
        }
        populator.close( true );
    }

    @Test
    public void dropShouldDeleteExistingFile() throws Exception
    {
        // given
        populator.create();

        // when
        populator.drop();

        // then
        assertFalse( fs.fileExists( indexFile ) );
    }

    @Test
    public void dropShouldSucceedOnNonExistentFile() throws Exception
    {
        // given
        assertFalse( fs.fileExists( indexFile ) );

        // then
        populator.drop();
    }

    @Test
    public void addShouldHandleEmptyCollection() throws Exception
    {
        // given
        populator.create();
        List<IndexEntryUpdate<?>> updates = Collections.emptyList();

        // when
        populator.add( updates );

        // then
        populator.close( true );
    }

    @Test
    public void addShouldApplyAllUpdatesOnce() throws Exception
    {
        // given
        populator.create();
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someIndexEntryUpdates();

        // when
        populator.add( Arrays.asList( updates ) );

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void updaterShouldApplyUpdates() throws Exception
    {
        // given
        populator.create();
        IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor );
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someIndexEntryUpdates();

        // when
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            updater.process( update );
        }

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void updaterMustThrowIfProcessAfterClose() throws Exception
    {
        // given
        populator.create();
        IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor );

        // when
        updater.close();

        // then
        try
        {
            updater.process( add( 1, Long.MAX_VALUE ) );
            fail( "Expected process to throw on closed updater" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
        populator.close( true );
    }

    @Test
    public void shouldApplyInterleavedUpdatesFromAddAndUpdater() throws Exception
    {
        // given
        populator.create();
        IndexUpdater updater = populator.newPopulatingUpdater( null_property_accessor );
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = someIndexEntryUpdates();

        // when
        applyInterleaved( updates, updater, populator );

        // then
        populator.close( true );
        verifyUpdates( updates );
    }

    @Test
    public void successfulCloseMustCloseGBPTree() throws Exception
    {
        // given
        populator.create();
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( indexFile );
        if ( existingMapping.isPresent() )
        {
            existingMapping.get().close();
        }
        else
        {
            fail( "Expected underlying GBPTree to have a mapping for this file" );
        }

        // when
        populator.close( true );

        // then
        existingMapping = pageCache.getExistingMapping( indexFile );
        assertFalse( existingMapping.isPresent() );
    }

    @Test
    public void successfulCloseMustMarkIndexAsOnline() throws Exception
    {
        // given
        populator.create();

        // when
        populator.close( true );

        // then
        assertHeader( true, null, false );
    }

    @Test
    public void unsuccessfulCloseMustSucceedWithoutMarkAsFailed() throws Exception
    {
        // given
        populator.create();

        // then
        populator.close( false );
    }

    @Test
    public void unsuccessfulCloseMustCloseGBPTree() throws Exception
    {
        // given
        populator.create();
        Optional<PagedFile> existingMapping = pageCache.getExistingMapping( indexFile );
        if ( existingMapping.isPresent() )
        {
            existingMapping.get().close();
        }
        else
        {
            fail( "Expected underlying GBPTree to have a mapping for this file" );
        }

        // when
        populator.close( false );

        // then
        existingMapping = pageCache.getExistingMapping( indexFile );
        assertFalse( existingMapping.isPresent() );
    }

    @Test
    public void unsuccessfulCloseMustNotMarkIndexAsOnline() throws Exception
    {
        // given
        populator.create();

        // when
        populator.close( false );

        // then
        assertHeader( false, "", false );
    }

    @Test
    public void closeMustWriteFailureMessageAfterMarkedAsFailed() throws Exception
    {
        // given
        populator.create();

        // when
        String failureMessage = "Fly, you fools!";
        populator.markAsFailed( failureMessage );
        populator.close( false );

        // then
        assertHeader( false, failureMessage, false );
    }

    @Test
    public void closeMustWriteFailureMessageAfterMarkedAsFailedWithLongMessage() throws Exception
    {
        // given
        populator.create();

        // when
        String failureMessage = longString( pageCache.pageSize() );
        populator.markAsFailed( failureMessage );
        populator.close( false );

        // then
        assertHeader( false, failureMessage, true );
    }

    @Test
    public void successfulCloseMustThrowIfMarkedAsFailed() throws Exception
    {
        // given
        populator.create();

        // when
        populator.markAsFailed( "" );

        // then
        try
        {
            populator.close( true );
            fail( "Expected successful close to fail after markedAsFailed" );
        }
        catch ( IllegalStateException e )
        {
            // good
        }
        populator.close( false );
    }

    @Test
    public void shouldApplyLargeAmountOfInterleavedRandomUpdates() throws Exception
    {
        // given
        populator.create();
        random.reset();
        Random updaterRandom = new Random( random.seed() );
        Iterator<IndexEntryUpdate<IndexDescriptor>> updates = randomUniqueUpdateGenerator( random, 0 );
        int numberOfPopulatorUpdates = LARGE_AMOUNT_OF_UPDATES;

        // when
        int count = 0;
        for ( int i = 0; i < numberOfPopulatorUpdates; i++ )
        {
            if ( updaterRandom.nextFloat() < 0.1 )
            {
                try ( IndexUpdater indexUpdater = populator.newPopulatingUpdater( null_property_accessor ) )
                {
                    int numberOfUpdaterUpdates = updaterRandom.nextInt( 100 );
                    for ( int j = 0; j < numberOfUpdaterUpdates; j++ )
                    {
                        indexUpdater.process( updates.next() );
                        count++;
                    }
                }
            }
            populator.add( updates.next() );
            count++;
        }

        // then
        populator.close( true );
        random.reset();
        verifyUpdates( randomUniqueUpdateGenerator( random, 0 ), count );
    }

    protected Iterator<IndexEntryUpdate<IndexDescriptor>> randomUniqueUpdateGenerator( RandomRule randomRule,
            float fractionDuplicates )
    {
        return new PrefetchingIterator<IndexEntryUpdate<IndexDescriptor>>()
        {
            private final Set<Double> uniqueCompareValues = new HashSet<>();
            private final List<Number> uniqueValues = new ArrayList<>();
            private long currentEntityId;

            @Override
            protected IndexEntryUpdate<IndexDescriptor> fetchNextOrNull()
            {
                Number value;
                if ( fractionDuplicates > 0 && !uniqueValues.isEmpty() &&
                        randomRule.nextFloat() < fractionDuplicates )
                {
                    value = existingNonUniqueValue( randomRule );
                }
                else
                {
                    value = newUniqueValue( randomRule );
                }

                return add( currentEntityId++, value );
            }

            private Number newUniqueValue( RandomRule randomRule )
            {
                Number value;
                Double compareValue;
                do
                {
                    value = randomRule.numberPropertyValue();
                    compareValue = value.doubleValue();
                }
                while ( !uniqueCompareValues.add( compareValue ) );
                uniqueValues.add( value );
                return value;
            }

            private Number existingNonUniqueValue( RandomRule randomRule )
            {
                return uniqueValues.get( randomRule.nextInt( uniqueValues.size() ) );
            }
        };
    }

    @SuppressWarnings( "rawtypes" )
    // unique ->
    // addShouldThrowOnDuplicateValues
    // updaterShouldThrowOnDuplicateValues
    // non-unique ->
    // addShouldApplyDuplicateValues
    // updaterShouldApplyDuplicateValues

    // successfulCloseMustCloseGBPTree
    // successfulCloseMustCheckpointGBPTree (already verified by add / updater tests)
    // successfulCloseMustMarkIndexAsOnline

    // unsuccessfulCloseMustCloseGBPTree
    // unsuccessfulCloseMustNotMarkIndexAsOnline
    // unsuccessfulCloseMustSucceedWithoutMarkAsFailed

    // closeMustWriteFailureMessageAfterMarkedAsFailed
    // closeMustWriteFailureMessageAfterMarkedAsFailedWithLongMessage
    // successfulCloseMustThrowIfMarkedAsFailed

    // shouldApplyLargeAmountOfInterleavedRandomUpdates
    // unique ->
    // shouldThrowOnLargeAmountOfInterleavedRandomUpdatesWithDuplicates
    // non-unique ->
    // shouldApplyLargeAmountOfInterleavedRandomUpdatesWithDuplicates

    // SAMPLING
    // includeSample
    // configureSampling
    // sampleResult

    // ???
    // todo closeAfterDrop
    // todo dropAfterClose

    // METHODS
    // create()
    // drop()
    // add( Collection<? extends IndexEntryUpdate<?>> updates )
    // add( IndexEntryUpdate<?> update )
    // verifyDeferredConstraints( PropertyAccessor propertyAccessor )
    // newPopulatingUpdater( PropertyAccessor accessor )
    // close( boolean populationCompletedSuccessfully )
    // markAsFailed( String failure )

    static IndexEntryUpdate[] someIndexEntryUpdates()
    {
        return new IndexEntryUpdate[]{
                add( 0, 0 ),
                add( 1, 4 ),
                add( 2, Double.MAX_VALUE ),
                add( 3, -Double.MAX_VALUE ),
                add( 4, Float.MAX_VALUE ),
                add( 5, -Float.MAX_VALUE ),
                add( 6, Long.MAX_VALUE ),
                add( 7, Long.MIN_VALUE ),
                add( 8, Integer.MAX_VALUE ),
                add( 9, Integer.MIN_VALUE ),
                add( 10, Short.MAX_VALUE ),
                add( 11, Short.MIN_VALUE ),
                add( 12, Byte.MAX_VALUE ),
                add( 13, Byte.MIN_VALUE )
        };
    }

    @SuppressWarnings( "rawtypes" )
    static IndexEntryUpdate[] someDuplicateIndexEntryUpdates()
    {
        return new IndexEntryUpdate[]{
                add( 0, 0 ),
                add( 1, 4 ),
                add( 2, Double.MAX_VALUE ),
                add( 3, -Double.MAX_VALUE ),
                add( 4, Float.MAX_VALUE ),
                add( 5, -Float.MAX_VALUE ),
                add( 6, Long.MAX_VALUE ),
                add( 7, Long.MIN_VALUE ),
                add( 8, Integer.MAX_VALUE ),
                add( 9, Integer.MIN_VALUE ),
                add( 10, Short.MAX_VALUE ),
                add( 11, Short.MIN_VALUE ),
                add( 12, Byte.MAX_VALUE ),
                add( 13, Byte.MIN_VALUE ),
                add( 14, 0 ),
                add( 15, 4 ),
                add( 16, Double.MAX_VALUE ),
                add( 17, -Double.MAX_VALUE ),
                add( 18, Float.MAX_VALUE ),
                add( 19, -Float.MAX_VALUE ),
                add( 20, Long.MAX_VALUE ),
                add( 21, Long.MIN_VALUE ),
                add( 22, Integer.MAX_VALUE ),
                add( 23, Integer.MIN_VALUE ),
                add( 24, Short.MAX_VALUE ),
                add( 25, Short.MIN_VALUE ),
                add( 26, Byte.MAX_VALUE ),
                add( 27, Byte.MIN_VALUE )
        };
    }

    private void assertHeader( boolean online, String failureMessage, boolean messageTruncated ) throws IOException
    {
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        try ( GBPTree<KEY,VALUE> tree = new GBPTree<>( pageCache, indexFile, layout, 0, GBPTree.NO_MONITOR,
                headerReader, RecoveryCleanupWorkCollector.IMMEDIATE ) )
        {
            if ( online )
            {
                assertEquals( "Index was not marked as online when expected not to be.", BYTE_ONLINE, headerReader.state );
                assertNull( "Expected failure message to be null when marked as online.", headerReader.failureMessage );
            }
            else
            {
                assertEquals( "Index was marked as online when expected not to be.", BYTE_FAILED, headerReader.state );
                if ( messageTruncated )
                {
                    assertTrue( headerReader.failureMessage.length() < failureMessage.length() );
                    assertTrue( failureMessage.startsWith( headerReader.failureMessage ) );
                }
                else
                {
                    assertEquals( failureMessage, headerReader.failureMessage );
                }
            }
        }
    }

    private String longString( int length )
    {
        String alphabet = "123xyz";
        StringBuffer outputBuffer = new StringBuffer( length );
        for ( int i = 0; i < length; i++ )
        {
            outputBuffer.append( alphabet.charAt( random.nextInt( alphabet.length() ) ) );
        }
        return outputBuffer.toString();
    }

    void applyInterleaved( IndexEntryUpdate<IndexDescriptor>[] updates, IndexUpdater updater,
            NativeSchemaIndexPopulator<KEY,VALUE> populator ) throws IOException, IndexEntryConflictException
    {
        for ( IndexEntryUpdate<IndexDescriptor> update : updates )
        {
            if ( random.nextBoolean() )
            {
                populator.add( update );
            }
            else
            {
                updater.process( update );
            }
        }
    }

    protected void verifyUpdates( Iterator<IndexEntryUpdate<IndexDescriptor>> indexEntryUpdateIterator, int count )
            throws IOException
    {
        @SuppressWarnings( "unchecked" )
        IndexEntryUpdate<IndexDescriptor>[] updates = new IndexEntryUpdate[count];
        for ( int i = 0; i < count; i++ )
        {
            updates[i] = indexEntryUpdateIterator.next();
        }
        verifyUpdates( updates );
    }

    @SuppressWarnings( "unchecked" )
    void verifyUpdates( IndexEntryUpdate<IndexDescriptor>[] updates )
            throws IOException
    {
        Hit<KEY,VALUE>[] expectedHits = convertToHits( updates, layout );
        List<Hit<KEY,VALUE>> actualHits = new ArrayList<>();
        try ( GBPTree<KEY,VALUE> tree = new GBPTree<>( pageCache, indexFile, layout, 0, GBPTree.NO_MONITOR,
                GBPTree.NO_HEADER, RecoveryCleanupWorkCollector.IMMEDIATE );
              RawCursor<Hit<KEY,VALUE>,IOException> scan = scan( tree ) )
        {
            while ( scan.next() )
            {
                actualHits.add( deepCopy( scan.get() ) );
            }
        }

        Comparator<Hit<KEY,VALUE>> hitComparator = ( h1, h2 ) ->
        {
            int keyCompare = layout.compare( h1.key(), h2.key() );
            if ( keyCompare == 0 )
            {
                return compareValue( h1.value(), h2.value() );
            }
            else
            {
                return keyCompare;
            }
        };
        assertSameHits( expectedHits, actualHits.toArray( new Hit[0] ), hitComparator );
    }

    protected abstract int compareValue( VALUE value1, VALUE value2 );

    int compareIndexedPropertyValue( SchemaNumberValue value1, SchemaNumberValue value2 )
    {
        int typeCompare = Byte.compare( value1.type(), value2.type() );
        if ( typeCompare == 0 )
        {
            switch ( value1.type() )
            {
            case LONG:
                return Long.compare( value1.rawValueBits(), value2.rawValueBits() );
            case FLOAT:
                return Float.compare(
                        Float.intBitsToFloat( (int) value1.rawValueBits() ),
                        Float.intBitsToFloat( (int) value2.rawValueBits() ) );
            case DOUBLE:
                return Double.compare(
                        Double.longBitsToDouble( value1.rawValueBits() ),
                        Double.longBitsToDouble( value2.rawValueBits() ) );
            default:
                throw new IllegalArgumentException(
                        "Expected type to be LONG, FLOAT or DOUBLE (" + LONG + "," + FLOAT + "," + DOUBLE +
                                "). But was " + value1.type() );
            }
        }
        return typeCompare;
    }

    private void assertSameHits( Hit<KEY, VALUE>[] expectedHits, Hit<KEY, VALUE>[] actualHits,
            Comparator<Hit<KEY, VALUE>> comparator )
    {
        Arrays.sort( expectedHits, comparator );
        Arrays.sort( actualHits, comparator );
        assertEquals( "Array length differ", expectedHits.length, actualHits.length );

        for ( int i = 0; i < expectedHits.length; i++ )
        {
            Hit<KEY,VALUE> expected = expectedHits[i];
            Hit<KEY,VALUE> actual = actualHits[i];
            assertTrue( "Hits differ on item number " + i + ". Expected " + expected + " but was " + actual,
                    comparator.compare( expected, actual ) == 0 );
        }
    }

    private Hit<KEY,VALUE> deepCopy( Hit<KEY,VALUE> from )
    {
        KEY intoKey = layout.newKey();
        VALUE intoValue = layout.newValue();
        layout.copyKey( from.key(), intoKey );
        copyValue( from.value(), intoValue );
        return new SimpleHit( intoKey, intoValue );
    }

    protected abstract void copyValue( VALUE value, VALUE intoValue );

    @SuppressWarnings( "unchecked" )
    private Hit<KEY,VALUE>[] convertToHits( IndexEntryUpdate<IndexDescriptor>[] updates,
            Layout<KEY,VALUE> layout )
    {
        List<Hit<KEY,VALUE>> hits = new ArrayList<>( updates.length );
        for ( IndexEntryUpdate<IndexDescriptor> u : updates )
        {
            KEY key = layout.newKey();
            key.from( u.getEntityId(), u.values() );
            VALUE value = layout.newValue();
            value.from( u.getEntityId(), u.values() );
            hits.add( hit( key, value ) );
        }
        return hits.toArray( new Hit[0] );
    }

    private Hit<KEY,VALUE> hit( final KEY key, final VALUE value )
    {
        return new SimpleHit( key, value );
    }

    private static class NativeSchemaIndexHeaderReader implements Header.Reader
    {
        byte state;
        String failureMessage;

        @Override
        public void read( PageCursor from, int length )
        {
            state = from.getByte();
            if ( state == NativeSchemaIndexPopulator.BYTE_FAILED )
            {
                short messageLength = from.getShort();
                byte[] failureMessageBytes = new byte[messageLength];
                from.getBytes( failureMessageBytes );
                failureMessage = new String( failureMessageBytes, Charsets.UTF_8 );
            }
        }
    }

    private class SimpleHit implements Hit<KEY,VALUE>
    {
        private final KEY key;
        private final VALUE value;

        SimpleHit( KEY key, VALUE value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public KEY key()
        {
            return key;
        }

        @Override
        public VALUE value()
        {
            return value;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            @SuppressWarnings( "unchecked" )
            Hit<KEY,VALUE> simpleHit = (Hit<KEY,VALUE>) o;
            return Objects.equals( key(), simpleHit.key() ) &&
                    Objects.equals( value, simpleHit.value() );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( key, value );
        }

        @Override
        public String toString()
        {
            return "[" + key + "," + value + "]";
        }
    }

    private RawCursor<Hit<KEY,VALUE>, IOException> scan( GBPTree<KEY,VALUE> tree ) throws IOException
    {
//        tree.printTree( false, false, false );
        KEY lowest = layout.newKey();
        lowest.initAsLowest();
        KEY highest = layout.newKey();
        highest.initAsHighest();
        return tree.seek( lowest, highest );
    }

    protected static IndexEntryUpdate<IndexDescriptor> add( long nodeId, Object value )
    {
        return IndexEntryUpdate.add( nodeId, indexDescriptor, value );
    }

    private byte[] fileWithContent() throws IOException
    {
        int size = 1000;
        try ( StoreChannel storeChannel = fs.create( indexFile ) )
        {
            byte[] someBytes = new byte[size];
            random.nextBytes( someBytes );
            storeChannel.writeAll( ByteBuffer.wrap( someBytes ) );
            return someBytes;
        }
    }
}
