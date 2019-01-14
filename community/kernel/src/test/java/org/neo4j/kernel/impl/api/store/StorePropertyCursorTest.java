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
package org.neo4j.kernel.impl.api.store;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.test.MockedNeoStores;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.apache.commons.lang3.RandomUtils.nextBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.AssertOpen.ALWAYS_OPEN;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@RunWith( Enclosed.class )
public class StorePropertyCursorTest
{
    private static final List<Object[]> PARAMETERS = asList(
            new Object[]{false, PropertyType.BOOL},
            new Object[]{(byte) 3, PropertyType.BYTE},
            new Object[]{(short) 34, PropertyType.SHORT},
            new Object[]{3456, PropertyType.INT},
            new Object[]{3456L, PropertyType.LONG},
            new Object[]{Integer.MAX_VALUE * 2L, PropertyType.LONG},
            new Object[]{1.6f, PropertyType.FLOAT},
            new Object[]{1.9d, PropertyType.DOUBLE},
            new Object[]{'a', PropertyType.CHAR},
            new Object[]{"short", PropertyType.SHORT_STRING},
            new Object[]{"notsoshort", PropertyType.SHORT_STRING},
            new Object[]{"alongershortstring", PropertyType.SHORT_STRING},
            new Object[]{"areallylongshortstringbutstillnotsobig", PropertyType.SHORT_STRING},
            new Object[]{Values.pointValue( CoordinateReferenceSystem.WGS84, 1.234, 4.321 ), PropertyType.GEOMETRY},
            new Object[]{Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 1.234, 4.321, -6.543 ), PropertyType.GEOMETRY},
            new Object[]{new double[]{0.0d}, PropertyType.SHORT_ARRAY},
            new Object[]{new double[]{1.2d}, PropertyType.SHORT_ARRAY},
            new Object[]{new double[]{1.2d, 1.4d}, PropertyType.SHORT_ARRAY},
            new Object[]{new double[]{1.2d, 1.4d, 1.6d}, PropertyType.SHORT_ARRAY},
            new Object[]{new double[]{1.2d, 1.4d, 1.6d, 1.8d}, PropertyType.ARRAY},
            new Object[]{new double[]{1.2d, 1.4d, 1.6d, 1.8d, 2.2d, 2.4d, 2.6d, 2.8d, 3.2d, 3.4d, 3.6d, 3.8d},
                    PropertyType.ARRAY},
            new Object[]{new PointValue[]{
                    Values.pointValue( CoordinateReferenceSystem.WGS84, 1.234, 4.321 ),
                    Values.pointValue( CoordinateReferenceSystem.WGS84, 4.321, -6.543 )
            }, PropertyType.ARRAY},
            new Object[]{new PointValue[]{
                    Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 3.987, 1.234, 4.321 ),
                    Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 1.234, 4.321, -6.543 )
            }, PropertyType.ARRAY},

            new Object[]{"thisisaveryveryveryverylongstringwhichisnotgonnafiteverintothepropertyblock",
                    PropertyType.STRING},
            new Object[]{new BigProperty(
                         "thisisaveryveryveryverylongstringwhichisnotgonnafiteverintothepropertyblock" + "\n" +
                         "thisisaveryveryveryverylongstringwhichisnotgonnafiteverintothepropertyblock",
                         "two very long lines..." ),
                    PropertyType.STRING},
            new Object[]{new BigProperty(
                         "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec ornare augue a felis" +
                         " interdum, id sodales magna tempor. Donec aliquam, nunc eu semper semper, orci " +
                         "metus tincidunt urna, non sagittis eros tellus vel tellus. Maecenas vel nisi magna." +
                         " Morbi tincidunt pretium nibh, eu tristique magna cursus vitae. Sed vel ultricies " +
                         "sem. Nunc blandit nulla leo, a tempor libero placerat ut. Cum sociis natoque " +
                         "penatibus et magnis dis parturient montes, nascetur ridiculus mus. Phasellus in " +
                         "velit vel dui euismod semper id varius neque. Class aptent taciti sociosqu ad " +
                         "litora torquent per conubia nostra, per inceptos himenaeos. Nam ultrices accumsan " +
                         "ultrices.\n" +
                         "\n" +
                         "Aenean laoreet tellus non velit vulputate finibus. Mauris facilisis mi ac eros " +
                         "hendrerit, mollis cursus lectus tincidunt. Sed et enim porta, mollis massa a, " +
                         "ornare lacus. Donec condimentum purus risus, ut vestibulum orci accumsan nec. " +
                         "Mauris condimentum aliquet felis, nec porttitor nunc faucibus vel. Donec eget " +
                         "rutrum urna. Donec aliquet, sapien quis ornare vulputate, tortor massa facilisis " +
                         "leo, nec pharetra dolor sapien non nunc. Mauris erat nulla, aliquam a sem sed, " +
                         "cursus ornare leo. Nulla et volutpat ligula. Curabitur iaculis massa vitae purus " +
                         "pretium, sed vehicula leo facilisis. Aenean egestas augue sit amet ex finibus, eu " +
                         "varius nisi tristique. Aenean molestie nisi vitae erat euismod pharetra.\n" +
                         "\n" +
                         "Nunc eu dolor euismod, commodo magna at, molestie velit. Cras vitae posuere sem, " +
                         "quis egestas mi. Morbi vestibulum, lorem sit amet semper porta, purus lorem rhoncus" +
                         " augue, non posuere justo magna sed diam. Donec a neque ac enim placerat semper eu " +
                         "ac purus. Duis sit amet sodales ligula. Vestibulum et dui tempus, molestie diam ut," +
                         " commodo felis. Sed at congue ligula, fermentum sagittis turpis.\n" +
                         "\n" +
                         "Fusce in pharetra nisl. Pellentesque urna urna, rutrum ac tellus sed, rutrum mattis" +
                         " magna. Donec facilisis, tellus consequat placerat mollis, mauris dui molestie " +
                         "ligula, ut ullamcorper lectus tortor in nisl. Aenean congue semper turpis. Mauris " +
                         "iaculis mi vel neque rutrum, vel viverra tellus hendrerit. Nam sed tincidunt lorem." +
                         " Vestibulum eleifend augue magna, nec gravida leo finibus id. Phasellus id arcu " +
                         "eget ipsum cursus placerat. Donec sit amet lorem porttitor nibh suscipit lobortis. " +
                         "Donec ultrices, purus nec convallis blandit, mi nisl tincidunt justo, dignissim " +
                         "maximus nisi nulla quis dui. Interdum et malesuada fames ac ante ipsum primis in " +
                         "faucibus. Integer ac rhoncus nunc. Donec varius tempus imperdiet. Aenean at semper " +
                         "elit. Donec mattis imperdiet sem. Ut dolor augue, bibendum et metus nec, vestibulum" +
                         " blandit dolor.\n" +
                         "\n" +
                         "Proin dui nisi, malesuada lacinia sodales sed, porta et arcu. Quisque in massa a " +
                         "diam ultrices porttitor. Mauris vulputate ipsum dignissim eros sodales facilisis in" +
                         " vel nunc. Fusce blandit efficitur convallis. Sed ut ex ac mauris dignissim tempor." +
                         " Morbi a dui nibh. Suspendisse eu lobortis lorem. Curabitur dictum convallis " +
                         "sapien, ac egestas odio hendrerit at.\n" +
                         "\n" +
                         "Donec nisi arcu, porta quis tristique ac, elementum vitae purus. Suspendisse tempor" +
                         " lorem eu metus gravida consectetur. Donec sapien felis, aliquam eget diam at, " +
                         "ultricies tristique velit. In libero velit, pulvinar accumsan fermentum a, mollis " +
                         "id risus. Praesent facilisis convallis dolor, et cursus tellus varius tristique. " +
                         "Vivamus ac eros pulvinar, blandit ipsum ac, interdum turpis. Donec nec ultrices " +
                         "elit. Proin auctor, nisl vitae viverra ornare, nisl ipsum congue ligula, et tempor " +
                         "diam orci ac ex. In faucibus massa quis purus malesuada convallis. Suspendisse " +
                         "metus ex, malesuada vel auctor et, finibus quis nulla. Cras tincidunt, mauris ac " +
                         "varius tincidunt, enim nunc finibus libero, et euismod quam dolor non magna. Nulla " +
                         "rhoncus dolor a nulla hendrerit iaculis. Nunc tristique, ante id tincidunt feugiat," +
                         " ligula ipsum faucibus dui, sed suscipit eros nulla non augue. Donec hendrerit arcu" +
                         " sit amet ex laoreet, sit amet gravida risus aliquam. Nullam efficitur placerat sem" +
                         " quis venenatis.\n" +
                         "\n" +
                         "Nullam scelerisque purus urna, vel laoreet velit consequat congue. Morbi tincidunt " +
                         "aliquet dignissim. Fusce neque mauris, euismod eu orci a, hendrerit pretium metus. " +
                         "In imperdiet nibh non augue pharetra, nec aliquet orci molestie. Morbi quis blandit" +
                         " leo. Mauris facilisis urna vitae ante molestie, ut efficitur dui elementum. " +
                         "Suspendisse a lectus sit amet turpis feugiat pellentesque ac vel nulla. Proin " +
                         "lobortis ante tincidunt porttitor aliquam. Praesent consequat blandit magna, sit " +
                         "amet finibus dui dapibus eget. Integer quis sem ut justo vulputate volutpat. " +
                         "Vestibulum nec varius lacus. Maecenas eget metus sed lectus suscipit laoreet. Sed " +
                         "mattis magna eu nunc ultrices vestibulum nec vel lacus. Curabitur dapibus nec arcu " +
                         "non tincidunt. Duis eget nulla dictum, lobortis leo ultrices, eleifend neque.\n" +
                         "\n" +
                         "Aliquam egestas tortor mi, sed blandit odio sollicitudin et. Mauris fermentum eros " +
                         "orci, id euismod ante interdum vel. Mauris egestas molestie augue, eu rutrum massa " +
                         "interdum in. Mauris sit amet facilisis risus, a convallis nunc. Vivamus hendrerit " +
                         "lobortis ex et ullamcorper. Sed suscipit egestas aliquet. Cras quis bibendum lacus." +
                         " Nulla maximus consectetur purus quis varius. Nullam ultricies vehicula lectus, " +
                         "eget elementum elit commodo sit amet. Quisque molestie finibus est vel bibendum. " +
                         "Vestibulum a imperdiet turpis, ut volutpat orci. Morbi erat augue, varius sed " +
                         "ullamcorper auctor, varius tincidunt ante. Vivamus mattis justo nulla, auctor " +
                         "euismod nulla mollis id. ", "Lorem ipsum... ad infinitum..." ),
                    PropertyType.STRING}
    );

    private StorePropertyCursorTest()
    {
    }

    /**
     * This is a work-around for a problem in Eclipse where a toString of a Parameter containing newline
     * would trigger a bug, making it impossible to run that test and any other test if this test
     * would be included in the set.
     */
    private static class BigProperty
    {
        private final Object actualValue;
        private final String toStringForUnitTest;

        BigProperty( Object value, String toStringForUnitTest )
        {
            this.actualValue = value;
            this.toStringForUnitTest = toStringForUnitTest;
        }

        Object value()
        {
            return actualValue;
        }

        @Override
        public String toString()
        {
            return toStringForUnitTest;
        }
    }

    /**
     * See {@link BigProperty} for explanation.
     */
    private static Object actualValue( Object parameter )
    {
        return parameter instanceof BigProperty ? ((BigProperty)parameter).value() : parameter;
    }

    public static class ErrorTest
    {
        private final NeoStores neoStores = MockedNeoStores.basicMockedNeoStores();
        private final PropertyStore propertyStore = neoStores.getPropertyStore();
        @SuppressWarnings( "unchecked" )
        private final Consumer<StorePropertyCursor> cache = mock( Consumer.class );

        {
            RecordCursor<PropertyRecord> recordCursor = MockedNeoStores.mockedRecordCursor();
            try
            {
                when( recordCursor.next() ).thenReturn( true );
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
            when( recordCursor.get() ).thenReturn( new PropertyRecord( 42 ) );
            when( propertyStore.newRecordCursor( any( PropertyRecord.class ) ) ).thenReturn( recordCursor );
        }

        @Test
        public void shouldReturnTheCursorToTheCacheOnClose()
        {
            // given
            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore, cache );
            storePropertyCursor.init( 0, NO_LOCK, ALWAYS_OPEN );

            // when
            storePropertyCursor.close();

            // then
            verify( cache, times( 1 ) ).accept( storePropertyCursor );
        }
    }

    public static class PropertyStoreBasedTestSupport
    {
        @ClassRule
        public static EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
        @ClassRule
        public static PageCacheRule pageCacheRule = new PageCacheRule();

        private static PageCache pageCache;
        private static NeoStores neoStores;
        protected static PropertyStore propertyStore;

        @BeforeClass
        public static void setUp()
        {
            pageCache = pageCacheRule.getPageCache( fsRule.get() );
            EphemeralFileSystemAbstraction fs = fsRule.get();
            File storeDir = new File( "store" ).getAbsoluteFile();
            if ( fs.isDirectory( storeDir ) )
            {
                fs.deleteRecursively( storeDir );
            }
            fs.mkdirs( storeDir );
            Config config = Config.defaults();
            DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
            NullLogProvider logProvider = NullLogProvider.getInstance();
            neoStores = new StoreFactory( storeDir, config, idGeneratorFactory, pageCache, fs, logProvider,
                    EmptyVersionContextSupplier.EMPTY )
                    .openAllNeoStores( true );
            propertyStore = neoStores.getPropertyStore();
        }

        @AfterClass
        public static void tearDown()
        {
            neoStores.close();
            pageCache.close();
        }

        @Test
        public void ignore()
        {
            // JUnit gets confused if this class has no method with the @Test annotation.
            // This is also why this class is not abstract.
        }
    }

    @RunWith( Parameterized.class )
    public static class SingleValueProperties extends PropertyStoreBasedTestSupport
    {
        @Parameterized.Parameter( 0 )
        public Object expectedValue;

        @Parameterized.Parameter( 1 )
        public PropertyType type;

        @Parameterized.Parameters( name = "value={0} of type={1}" )
        public static List<Object[]> parameters()
        {
            return PARAMETERS;
        }

        @Test
        public void shouldReturnAProperty()
        {
            // given
            int keyId = 11;
            Object expectedValue = actualValue( this.expectedValue );

            long recordId = createSinglePropertyValue( propertyStore, keyId, expectedValue ).getId();

            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore );

            // when
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK, ALWAYS_OPEN ) )
            {
                // then
                assertTrue( cursor.next() );
                PropertyItem item = cursor.get();
                assertEquals( keyId, item.propertyKeyId() );
                assertEqualValues( expectedValue, item );
                assertFalse( cursor.next() );
            }
        }
    }

    @RunWith( Parameterized.class )
    public static class TwoValueProperties extends PropertyStoreBasedTestSupport
    {
        @Parameterized.Parameter( 0 )
        public Object expectedValue1;

        @Parameterized.Parameter( 1 )
        public PropertyType type1;

        @Parameterized.Parameter( 2 )
        public Object expectedValue2;

        @Parameterized.Parameter( 3 )
        public PropertyType type2;

        @Parameterized.Parameters( name = "value={0} of type={1} and value={2} of type={3}" )
        public static Collection<Object[]> parameters()
        {
            List<Object[]> combinations = new ArrayList<>();
            for ( int i = 0; i < PARAMETERS.size(); i++ )
            {
                Object[] current = PARAMETERS.get( i );
                for ( Object[] other : PARAMETERS )
                {
                    Object[] objects = new Object[4];
                    objects[0] = current[0];
                    objects[1] = current[1];
                    objects[2] = other[0];
                    objects[3] = other[1];
                    combinations.add( objects );
                }

            }
            return combinations;
        }

        @Test
        public void shouldReturnAPropertyBySkippingOne()
        {
            // given
            int keyId1 = 11;
            int keyId2 = 22;
            Object expectedValue1 = actualValue( this.expectedValue1 );
            Object expectedValue2 = actualValue( this.expectedValue2 );

            long recordId = createTwoPropertyValues(
                    propertyStore, keyId1, expectedValue1, keyId2, expectedValue2 ).getId();

            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore );

            // when
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK, ALWAYS_OPEN ) )
            {
                // then
                assertTrue( cursor.next() );
                PropertyItem item = cursor.get();
                assertEquals( keyId1, item.propertyKeyId() );

                // skipping first property

                assertTrue( cursor.next() );
                assertEquals( keyId2, item.propertyKeyId() );
                assertEqualValues( expectedValue2, item );

                assertFalse( cursor.next() );
            }
        }

        @Test
        public void shouldReturnTwoProperties()
        {
            // given
            int keyId1 = 11;
            int keyId2 = 22;
            Object expectedValue1 = actualValue( this.expectedValue1 );
            Object expectedValue2 = actualValue( this.expectedValue2 );

            long recordId = createTwoPropertyValues(
                    propertyStore, keyId1, expectedValue1, keyId2, expectedValue2 ).getId();

            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore );

            // when
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK, ALWAYS_OPEN ) )
            {
                PropertyItem item;

                // then
                assertTrue( cursor.next() );
                item = cursor.get();
                assertEquals( keyId1, item.propertyKeyId() );
                assertEqualValues( expectedValue1, item );

                assertTrue( cursor.next() );
                item = cursor.get();
                assertEquals( keyId2, item.propertyKeyId() );
                assertEqualValues( expectedValue2, item );

                assertFalse( cursor.next() );
            }
        }
    }

    @RunWith( Parameterized.class )
    public static class CursorReuse extends PropertyStoreBasedTestSupport
    {
        @Parameterized.Parameter( 0 )
        public Object expectedValue;

        @Parameterized.Parameter( 1 )
        public PropertyType type;

        @Parameterized.Parameters( name = "value={0} of type={1}" )
        public static List<Object[]> parameters()
        {
            return PARAMETERS;
        }

        @Test
        public void shouldReuseCorrectlyCursor()
        {
            // given
            int keyId = 11;
            Object expectedValue = actualValue( this.expectedValue );

            long recordId = createSinglePropertyValue( propertyStore, keyId, expectedValue ).getId();

            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore );

            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK , ALWAYS_OPEN) )
            {
                assertTrue( cursor.next() );
                PropertyItem item = cursor.get();
                assertEquals( keyId, item.propertyKeyId() );
                assertEqualValues( expectedValue, item );
                assertFalse( cursor.next() );
            }

            // when using it
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK , ALWAYS_OPEN) )
            {
                // then
                assertTrue( cursor.next() );
                PropertyItem item = cursor.get();
                assertEquals( keyId, item.propertyKeyId() );
                assertEqualValues( expectedValue, item );
                assertFalse( cursor.next() );
            }
        }
    }

    public static class PropertyChains extends PropertyStoreBasedTestSupport
    {
        @Test
        public void readPropertyChainWithMultipleEntries()
        {
            int propertyKeyId = 42;
            Object[] propertyValues = {"1", "2", 3, 4, 5L, 6L, '7', '8', "9 and 10"};

            long firstPropertyId = firstIdOf( createPropertyChain( propertyStore, propertyKeyId, propertyValues ) );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK, ALWAYS_OPEN );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( asList( propertyValues ), valuesFromCursor );
            }
        }

        @Test
        public void callNextAfterReadingPropertyChain()
        {
            int propertyKeyId = 42;

            long firstPropertyId = firstIdOf( createPropertyChain( propertyStore, propertyKeyId, "1", "2" ) );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK, ALWAYS_OPEN );

                assertTrue( cursor.next() );
                assertEquals( Values.of( "1" ), cursor.value() );

                assertTrue( cursor.next() );
                assertEquals( Values.of( "2" ), cursor.value() );

                assertFalse( cursor.next() );
                assertFalse( cursor.next() );
                assertFalse( cursor.next() );
            }
        }

        @Test
        public void skipUnusedRecordsInChain()
        {
            int propertyKeyId = 42;
            Object[] propertyValues = {"1", "2", 3, 4, 5L, 6L, '7', '8', "9 and 10"};

            List<PropertyRecord> propertyChain = createPropertyChain( propertyStore, propertyKeyId, propertyValues );
            long firstPropertyId = firstIdOf( propertyChain );
            markPropertyRecordsNoInUse( propertyStore, idsOf( propertyChain, 1, 2, 4 ) );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK, ALWAYS_OPEN );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( asList( "1", 3, 5L, '7', "9 and 10" ), valuesFromCursor );
            }
        }

        @Test
        public void skipUnusedConsecutiveRecordsInChain()
        {
            int propertyKeyId = 42;
            Object[] propertyValues = {"1", "2", 3, 4, 5L, 6L, '7', '8', "9 and 10"};

            List<PropertyRecord> propertyChain = createPropertyChain( propertyStore, propertyKeyId, propertyValues );
            long firstPropertyId = firstIdOf( propertyChain );
            markPropertyRecordsNoInUse( propertyStore, idsOf( propertyChain, 2, 1, 3 ) );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK, ALWAYS_OPEN );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( asList( "1", "2", 6L, '7', '8', "9 and 10" ), valuesFromCursor );
            }
        }

        @Test
        public void skipAllRecordsWhenWholeChainNotInUse()
        {
            int propertyKeyId = 42;
            Object[] propertyValues = {"1", "2", 3, 4, 5L, 6L, '7', '8', "9 and 10"};

            List<PropertyRecord> propertyChain = createPropertyChain( propertyStore, propertyKeyId, propertyValues );
            long firstPropertyId = firstIdOf( propertyChain );
            int[] recordIds = idsOf( propertyChain, 0, 1, propertyValues.length );
            markPropertyRecordsNoInUse( propertyStore, recordIds );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK, ALWAYS_OPEN );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( Collections.emptyList(), valuesFromCursor );
            }
        }

        @Test
        public void readPropertyChainWithLongStringDynamicRecordsNotInUse()
        {
            int keyId = 42;
            Object[] values = {randomAscii( 255 ), randomAscii( 255 ), randomAscii( 255 )};
            List<PropertyRecord> propertyChain = createPropertyChain( propertyStore, keyId, values );
            long chainStartId = firstIdOf( propertyChain );

            markDynamicRecordsNotInUse( keyId, propertyChain.get( 1 ), propertyStore, 2 );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( chainStartId, NO_LOCK, ALWAYS_OPEN );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( asList( values ), valuesFromCursor );
            }
        }

        @Test
        public void readPropertyValueWhenFirstLongStringDynamicRecordIsNotInUse()
        {
            int keyId = 1;
            String value = randomAscii( 255 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, keyId, value );
            markDynamicRecordsNotInUse( keyId, record, propertyStore, 0 );

            verifyPropertyValue( value, record.getId() );
        }

        @Test
        public void readPropertyValueWhenSomeLongStringDynamicRecordsAreNotInUse()
        {
            int keyId = 1;
            String value = randomAscii( 1000 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, keyId, value );

            markDynamicRecordsNotInUse( keyId, record, propertyStore, 1, 3, 5, 7 );

            verifyPropertyValue( value, record.getId() );
        }

        @Test
        public void readPropertyValueWhenAllLongStringDynamicRecordsAreNotInUse()
        {
            int keyId = 1;
            String value = randomAscii( 255 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, keyId, value );

            markDynamicRecordsNotInUse( keyId, record, propertyStore, 0, 1, 2 );

            verifyPropertyValue( value, record.getId() );
        }

        @Test
        public void readPropertyValueWhenAllLongArrayDynamicRecordsAreNotInUse()
        {
            int keyId = 1;
            byte[] value = nextBytes( 320 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, keyId, value );

            markDynamicRecordsNotInUse( keyId, record, propertyStore, 0, 1, 2 );

            verifyPropertyValue( value, record.getId() );
        }

        @Test
        public void readPropertyChainWithLongArrayDynamicRecordsNotInUse()
        {
            int keyId = 42;
            Object[] values = {nextBytes( 1024 ), nextBytes( 1024 ), nextBytes( 1024 )};
            List<PropertyRecord> propertyChain = createPropertyChain( propertyStore, keyId, values );
            long chainStartId = firstIdOf( propertyChain );

            markDynamicRecordsNotInUse( keyId, propertyChain.get( 1 ), propertyStore, 2 );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( chainStartId, NO_LOCK, ALWAYS_OPEN );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                for ( int i = 0; i < valuesFromCursor.size(); i++ )
                {
                    Object value = valuesFromCursor.get( i );
                    assertArrayEquals( (byte[]) values[i], (byte[]) value );
                }
            }
        }

        @Test
        public void readPropertyValueWhenFirstLongArrayDynamicRecordIsNotInUse()
        {
            int keyId = 1;
            byte[] value = nextBytes( 1024 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, keyId, value );
            markDynamicRecordsNotInUse( keyId, record, propertyStore, 0 );

            verifyPropertyValue( value, record.getId() );
        }

        @Test
        public void readPropertyValueWhenSomeLongArrayDynamicRecordsAreNotInUse()
        {
            int keyId = 1;
            byte[] value = nextBytes( 1024 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, keyId, value );

            markDynamicRecordsNotInUse( keyId, record, propertyStore, 1, 3, 5, 7 );

            verifyPropertyValue( value, record.getId() );
        }

        private void verifyPropertyValue( String expectedValue, long recordId )
        {
            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( recordId, NO_LOCK, ALWAYS_OPEN );
                assertTrue( cursor.next() );
                assertEquals( Values.of( expectedValue ), cursor.value() );
                assertFalse( cursor.next() );
            }
        }

        private void verifyPropertyValue( byte[] expectedValue, long recordId )
        {
            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( recordId, NO_LOCK, ALWAYS_OPEN );
                assertTrue( cursor.next() );
                assertTrue( cursor.value().equals( expectedValue ) );
                assertFalse( cursor.next() );
            }
        }
    }

    private static void assertEqualValues( Object expectedValue, PropertyItem item )
    {
        // fetch twice with typed methods
        Value expected = Values.of( expectedValue );
        assertTrue( item.value().equals( expected ) );
        assertTrue( item.value().equals( expected ) );
    }

    public static long firstIdOf( List<PropertyRecord> propertyChain )
    {
        return propertyChain.get( 0 ).getId();
    }

    private static StorePropertyCursor newStorePropertyCursor( PropertyStore propertyStore )
    {
        return newStorePropertyCursor( propertyStore, ignored -> {} );
    }

    private static StorePropertyCursor newStorePropertyCursor( PropertyStore propertyStore,
            Consumer<StorePropertyCursor> cache )
    {
        RecordCursor<PropertyRecord> propertyRecordCursor = propertyStore.newRecordCursor( propertyStore.newRecord() );
        propertyRecordCursor.acquire( 0, NORMAL );

        DynamicStringStore stringStore = propertyStore.getStringStore();
        RecordCursor<DynamicRecord> dynamicStringCursor = stringStore.newRecordCursor( stringStore.nextRecord() );
        dynamicStringCursor.acquire( 0, NORMAL );

        DynamicArrayStore arrayStore = propertyStore.getArrayStore();
        RecordCursor<DynamicRecord> dynamicArrayCursor = arrayStore.newRecordCursor( arrayStore.nextRecord() );
        dynamicArrayCursor.acquire( 0, NORMAL );

        RecordCursors cursors = mock( RecordCursors.class );
        when( cursors.property() ).thenReturn( propertyRecordCursor );
        when( cursors.propertyString() ).thenReturn( dynamicStringCursor );
        when( cursors.propertyArray() ).thenReturn( dynamicArrayCursor );

        return new StorePropertyCursor( cursors, cache );
    }

    private static List<PropertyRecord> createPropertyChain( PropertyStore store, int keyId,
            Object... values )
    {
        List<PropertyRecord> records = new ArrayList<>();

        for ( Object value : values )
        {
            PropertyRecord record = createSinglePropertyValue( store, keyId, value );
            if ( !records.isEmpty() )
            {
                PropertyRecord previousRecord = records.get( records.size() - 1 );

                record.setPrevProp( previousRecord.getId() );
                store.updateRecord( record );

                previousRecord.setNextProp( record.getId() );
                store.updateRecord( previousRecord );
            }
            records.add( record );
        }

        return records;
    }

    private static void markPropertyRecordsNoInUse( PropertyStore store, int... recordIds )
    {
        for ( int recordId : recordIds )
        {
            PropertyRecord record = RecordStore.getRecord( store, recordId );
            record.setInUse( false );
            store.updateRecord( record );
        }
    }

    public static int[] idsOf( List<PropertyRecord> propertyChain, int startIndex, int step, int count )
    {
        int[] result = new int[count];
        for ( int i = 0; i < count; i++ )
        {
            result[i] = toIntExact( propertyChain.get( startIndex + i * step ).getId() );
        }
        return result;
    }

    private static PropertyRecord createSinglePropertyValue( PropertyStore store, int keyId,
            Object value )
    {
        DynamicRecordAllocator stringAllocator = store.getStringStore();
        DynamicRecordAllocator arrayAllocator = store.getArrayStore();

        PropertyBlock block = new PropertyBlock();
        PropertyStore.encodeValue( block, keyId, Values.of( value ), stringAllocator, arrayAllocator, true );

        PropertyRecord record = new PropertyRecord( store.nextId() );
        record.addPropertyBlock( block );
        record.setInUse( true );
        updateRecord( store, record );

        return record;
    }

    private static PropertyRecord createTwoPropertyValues( PropertyStore store,
            int keyId1, Object value1, int keyId2, Object value2 )
    {
        DynamicRecordAllocator stringAllocator = store.getStringStore();
        DynamicRecordAllocator arrayAllocator = store.getArrayStore();

        PropertyBlock block1 = new PropertyBlock();
        PropertyStore.encodeValue( block1, keyId1, Values.of( value1 ), stringAllocator, arrayAllocator, true );
        PropertyBlock block2 = new PropertyBlock();
        PropertyStore.encodeValue( block2, keyId2, Values.of( value2 ), stringAllocator, arrayAllocator, true );

        PropertyRecord record = new PropertyRecord( store.nextId() );
        record.addPropertyBlock( block1 );
        if ( block1.getSize() + block2.getSize() <= PropertyRecordFormat.DEFAULT_PAYLOAD_SIZE )
        {
            record.addPropertyBlock( block2 );
        }
        else
        {
            PropertyRecord nextRecord = new PropertyRecord( store.nextId() );
            record.setNextProp( nextRecord.getId() );
            nextRecord.addPropertyBlock( block2 );
            nextRecord.setPrevProp( record.getId() );
            nextRecord.setInUse( true );
            updateRecord( store, nextRecord );
        }

        record.setInUse( true );
        updateRecord( store, record );
        return record;
    }

    private static void markDynamicRecordsNotInUse( int keyId, PropertyRecord record,
            PropertyStore store, int... dynamicRecordIndexes )
    {
        PropertyBlock propertyBlock = record.getPropertyBlock( keyId );
        store.ensureHeavy( propertyBlock );
        List<DynamicRecord> valueRecords = propertyBlock.getValueRecords();
        for ( int index : dynamicRecordIndexes )
        {
            DynamicRecord dynamicRecord = valueRecords.get( index );
            dynamicRecord.setInUse( false );
        }
        updateRecord( store, record );
    }

    private static List<Object> asPropertyValuesList( StorePropertyCursor cursor )
    {
        List<Object> values = new ArrayList<>();
        while ( cursor.next() )
        {
            values.add( cursor.value().asObjectCopy() );
        }
        return values;
    }

    private static <T extends AbstractBaseRecord> void updateRecord( RecordStore<T> store, T record )
    {
        store.updateRecord( record );
        store.setHighestPossibleIdInUse( record.getId() );
    }
}
