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
package org.neo4j.kernel.impl.api.store;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.function.Consumers;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.impl.store.AbstractRecordStore;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.apache.commons.lang3.RandomUtils.nextBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;

@RunWith( Enclosed.class )
public class StorePropertyCursorTest
{
    private static final List<Object[]> PARAMETERS = asList(
            new Object[]{false, PropertyType.BOOL},
            new Object[]{(byte) 3, PropertyType.BYTE},
            new Object[]{(short) 34, PropertyType.SHORT},
            new Object[]{3456, PropertyType.INT},
            new Object[]{3456l, PropertyType.LONG},
            new Object[]{Integer.MAX_VALUE * 2l, PropertyType.LONG},
            new Object[]{1.6f, PropertyType.FLOAT},
            new Object[]{1.9d, PropertyType.DOUBLE},
            new Object[]{'a', PropertyType.CHAR},
            new Object[]{"short", PropertyType.SHORT_STRING},
            new Object[]{"notsoshort", PropertyType.SHORT_STRING},
            new Object[]{"alongershortstring", PropertyType.SHORT_STRING},
            new Object[]{"areallylongshortstringbutstillnotsobig", PropertyType.SHORT_STRING},
            new Object[]{new double[]{0.0d}, PropertyType.SHORT_ARRAY},
            new Object[]{new double[]{1.2d}, PropertyType.SHORT_ARRAY},
            new Object[]{new double[]{1.2d, 1.4d}, PropertyType.SHORT_ARRAY},
            new Object[]{new double[]{1.2d, 1.4d, 1.6d}, PropertyType.SHORT_ARRAY},
            new Object[]{new double[]{1.2d, 1.4d, 1.6d, 1.8d}, PropertyType.ARRAY},
            new Object[]{new double[]{1.2d, 1.4d, 1.6d, 1.8d, 2.2d, 2.4d, 2.6d, 2.8d, 3.2d, 3.4d, 3.6d, 3.8d},
                    PropertyType.ARRAY},
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
        private final PropertyStore propertyStore = mock( PropertyStore.class );
        @SuppressWarnings( "unchecked" )
        private final Consumer<StorePropertyCursor> cache = mock( Consumer.class );
        private final DynamicStringStore stringStore = mock( DynamicStringStore.class );
        private final DynamicArrayStore arrayStore = mock( DynamicArrayStore.class );

        {
            when( propertyStore.getStringStore() ).thenReturn( stringStore );
            when( propertyStore.getArrayStore() ).thenReturn( arrayStore );
        }

        @Test
        public void shouldReturnTheCursorToTheCacheOnClose() throws Throwable
        {
            // given
            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore, cache );
            storePropertyCursor.init( 0, NO_LOCK );

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

        @BeforeClass
        public static void setUpPageCache()
        {
            pageCache = pageCacheRule.getPageCache( fsRule.get() );
        }

        @AfterClass
        public static void tearDownPageCache() throws IOException
        {
            pageCache.close();
        }

        protected PropertyStore propertyStore;
        private NeoStores neoStores;

        @Before
        public void setup() throws IOException
        {
            EphemeralFileSystemAbstraction fs = fsRule.get();
            LogProvider log = NullLogProvider.getInstance();

            File storeDir = new File( "store" );
            if ( fs.isDirectory( storeDir ) )
            {
                fs.deleteRecursively( storeDir );
            }
            fs.mkdirs( storeDir );
            StoreFactory storeFactory = new StoreFactory( fs, storeDir, pageCache, log );
            neoStores = storeFactory.openAllNeoStores( true );
            propertyStore = neoStores.getPropertyStore();
        }

        @After
        public void tearDown()
        {
            neoStores.close();
        }

        @Test
        public void ignore() throws Exception
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
        public void shouldReturnAProperty() throws Throwable
        {
            // given
            int recordId = 42;
            int keyId = 11;
            Object expectedValue = actualValue( this.expectedValue );

            createSinglePropertyValue( propertyStore, recordId, keyId, expectedValue );

            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore );

            // when
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK ) )
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
        public void shouldReturnAPropertyBySkippingOne() throws Throwable
        {
            // given
            int recordId = 42;
            int keyId1 = 11;
            int keyId2 = 22;
            Object expectedValue1 = actualValue( this.expectedValue1 );
            Object expectedValue2 = actualValue( this.expectedValue2 );

            createTwoPropertyValues( propertyStore, recordId, keyId1, expectedValue1, keyId2, expectedValue2 );

            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore );

            // when
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK ) )
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
        public void shouldReturnTwoProperties() throws Throwable
        {
            // given
            int recordId = 42;
            int keyId1 = 11;
            int keyId2 = 22;
            Object expectedValue1 = actualValue( this.expectedValue1 );
            Object expectedValue2 = actualValue( this.expectedValue2 );

            createTwoPropertyValues( propertyStore, recordId, keyId1, expectedValue1, keyId2, expectedValue2 );

            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore );

            // when
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK ) )
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
        public void shouldReuseCorrectlyCursor() throws Throwable
        {
            // given
            int recordId = 42;
            int keyId = 11;
            Object expectedValue = actualValue( this.expectedValue );

            createSinglePropertyValue( propertyStore, recordId, keyId, expectedValue );

            StorePropertyCursor storePropertyCursor = newStorePropertyCursor( propertyStore );

            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK ) )
            {
                assertTrue( cursor.next() );
                PropertyItem item = cursor.get();
                assertEquals( keyId, item.propertyKeyId() );
                assertEqualValues( expectedValue, item );
                assertFalse( cursor.next() );
            }

            // when using it
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId, NO_LOCK ) )
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
            int firstPropertyId = 1;
            int propertyKeyId = 42;
            Object[] propertyValues = {"1", "2", 3, 4, 5L, 6L, '7', '8', "9 and 10"};

            createPropertyChain( propertyStore, firstPropertyId, propertyKeyId, propertyValues );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( asList( propertyValues ), valuesFromCursor );
            }
        }

        @Test
        public void callNextAfterReadingPropertyChain()
        {
            int firstPropertyId = 1;
            int propertyKeyId = 42;

            createPropertyChain( propertyStore, firstPropertyId, propertyKeyId, "1", "2" );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK );

                assertTrue( cursor.next() );
                assertEquals( "1", cursor.value() );

                assertTrue( cursor.next() );
                assertEquals( "2", cursor.value() );

                assertFalse( cursor.next() );
                assertFalse( cursor.next() );
                assertFalse( cursor.next() );
            }
        }

        @Test
        public void skipUnusedRecordsInChain()
        {
            int firstPropertyId = 1;
            int propertyKeyId = 42;
            Object[] propertyValues = {"1", "2", 3, 4, 5L, 6L, '7', '8', "9 and 10"};

            createPropertyChain( propertyStore, firstPropertyId, propertyKeyId, propertyValues );
            markPropertyRecordsNoInUse( propertyStore, generateRecordIds( firstPropertyId + 1, 2, 4 ) );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( asList( "1", 3, 5L, '7', "9 and 10" ), valuesFromCursor );
            }
        }

        @Test
        public void skipUnusedConsecutiveRecordsInChain()
        {
            int firstPropertyId = 1;
            int propertyKeyId = 42;
            Object[] propertyValues = {"1", "2", 3, 4, 5L, 6L, '7', '8', "9 and 10"};

            createPropertyChain( propertyStore, firstPropertyId, propertyKeyId, propertyValues );
            markPropertyRecordsNoInUse( propertyStore, generateRecordIds( firstPropertyId + 2, 1, 3 ) );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( asList( "1", "2", 6L, '7', '8', "9 and 10" ), valuesFromCursor );
            }
        }

        @Test
        public void skipAllRecordsWhenWholeChainNotInUse()
        {
            int firstPropertyId = 1;
            int propertyKeyId = 42;
            Object[] propertyValues = {"1", "2", 3, 4, 5L, 6L, '7', '8', "9 and 10"};

            createPropertyChain( propertyStore, firstPropertyId, propertyKeyId, propertyValues );
            int[] recordIds = generateRecordIds( firstPropertyId, 1, propertyValues.length );
            markPropertyRecordsNoInUse( propertyStore, recordIds );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( firstPropertyId, NO_LOCK );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( Collections.emptyList(), valuesFromCursor );
            }
        }

        @Test
        public void readPropertyChainWithLongStringDynamicRecordsNotInUse()
        {
            int chainStartId = 1;
            int keyId = 42;
            Object[] values = {randomAscii( 255 ), randomAscii( 255 ), randomAscii( 255 )};
            List<PropertyRecord> propertyChain = createPropertyChain( propertyStore, chainStartId, keyId, values );

            markDynamicRecordNotInUse( 2, keyId, propertyChain.get( 1 ), propertyStore );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( chainStartId, NO_LOCK );

                List<Object> valuesFromCursor = asPropertyValuesList( cursor );
                assertEquals( asList( values ), valuesFromCursor );
            }
        }

        @Test
        public void readPropertyValueWhenFirstLongStringDynamicRecordIsNotInUse()
        {
            int recordId = 1;
            int keyId = 1;
            String value = randomAscii( 255 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, recordId, keyId, value );
            markDynamicRecordNotInUse( 0, keyId, record, propertyStore );

            verifyPropertyValue( value, recordId );
        }

        @Test
        public void readPropertyValueWhenSomeLongStringDynamicRecordsAreNotInUse()
        {
            int recordId = 1;
            int keyId = 1;
            String value = randomAscii( 1000 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, recordId, keyId, value );

            markDynamicRecordNotInUse( 1, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 3, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 5, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 7, keyId, record, propertyStore );

            verifyPropertyValue( value, recordId );
        }

        @Test
        public void readPropertyValueWhenAllLongStringDynamicRecordsAreNotInUse()
        {
            int recordId = 1;
            int keyId = 1;
            String value = randomAscii( 255 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, recordId, keyId, value );

            markDynamicRecordNotInUse( 0, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 1, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 2, keyId, record, propertyStore );

            verifyPropertyValue( value, recordId );
        }

        @Test
        public void readPropertyValueWhenAllLongArrayDynamicRecordsAreNotInUse()
        {
            int recordId = 1;
            int keyId = 1;
            byte[] value = nextBytes( 320 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, recordId, keyId, value );

            markDynamicRecordNotInUse( 0, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 1, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 2, keyId, record, propertyStore );

            verifyPropertyValue( value, recordId );
        }

        @Test
        public void readPropertyChainWithLongArrayDynamicRecordsNotInUse()
        {
            int chainStartId = 1;
            int keyId = 42;
            Object[] values = {nextBytes( 1024 ), nextBytes( 1024 ), nextBytes( 1024 )};
            List<PropertyRecord> propertyChain = createPropertyChain( propertyStore, chainStartId, keyId, values );

            markDynamicRecordNotInUse( 2, keyId, propertyChain.get( 1 ), propertyStore );

            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( chainStartId, NO_LOCK );

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
            int recordId = 1;
            int keyId = 1;
            byte[] value = nextBytes( 1024 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, recordId, keyId, value );
            markDynamicRecordNotInUse( 0, keyId, record, propertyStore );

            verifyPropertyValue( value, recordId );
        }

        @Test
        public void readPropertyValueWhenSomeLongArrayDynamicRecordsAreNotInUse()
        {
            int recordId = 1;
            int keyId = 1;
            byte[] value = nextBytes( 1024 );
            PropertyRecord record = createSinglePropertyValue( propertyStore, recordId, keyId, value );

            markDynamicRecordNotInUse( 1, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 3, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 5, keyId, record, propertyStore );
            markDynamicRecordNotInUse( 7, keyId, record, propertyStore );

            verifyPropertyValue( value, recordId );
        }

        private void verifyPropertyValue( String expectedValue, int recordId )
        {
            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( recordId, NO_LOCK );
                assertTrue( cursor.next() );
                assertEquals( expectedValue, cursor.value() );
                assertFalse( cursor.next() );
            }
        }

        private void verifyPropertyValue( byte[] expectedValue, int recordId )
        {
            try ( StorePropertyCursor cursor = newStorePropertyCursor( propertyStore ) )
            {
                cursor.init( recordId, NO_LOCK );
                assertTrue( cursor.next() );
                assertArrayEquals( expectedValue, (byte[]) cursor.value() );
                assertFalse( cursor.next() );
            }
        }
    }

    private static void assertEqualValues( Object expectedValue, PropertyItem item )
    {
        // fetch twice with typed methods
        if ( expectedValue.getClass().isArray() )
        {
            assertArrayEquals( (double[]) expectedValue, (double[]) item.value(), 0.0 );
            assertArrayEquals( (double[]) expectedValue, (double[]) item.value(), 0.0 );
        }
        else
        {
            assertEquals( expectedValue, item.value() );
            assertEquals( expectedValue, item.value() );
        }
    }

    private static StorePropertyCursor newStorePropertyCursor( PropertyStore propertyStore )
    {
        return newStorePropertyCursor( propertyStore, Consumers.<StorePropertyCursor>noop() );
    }

    private static StorePropertyCursor newStorePropertyCursor( PropertyStore propertyStore,
            Consumer<StorePropertyCursor> cache )
    {
        return new StorePropertyCursor( propertyStore, cache );
    }

    private static List<PropertyRecord> createPropertyChain( PropertyStore store, int firstRecordId, int keyId,
            Object... values )
    {
        List<PropertyRecord> records = new ArrayList<>();

        int nextRecordId = firstRecordId;
        for ( Object value : values )
        {
            PropertyRecord record = createSinglePropertyValue( store, nextRecordId, keyId, value );
            if ( !records.isEmpty() )
            {
                PropertyRecord previousRecord = records.get( records.size() - 1 );

                record.setPrevProp( previousRecord.getId() );
                store.updateRecord( record );

                previousRecord.setNextProp( record.getId() );
                store.updateRecord( previousRecord );
            }
            records.add( record );
            nextRecordId++;
        }

        return records;
    }

    private static void markPropertyRecordsNoInUse( PropertyStore store, int... recordIds )
    {
        for ( int recordId : recordIds )
        {
            PropertyRecord record = store.forceGetRecord( recordId );
            record.setInUse( false );
            store.forceUpdateRecord( record );
        }
    }

    private static int[] generateRecordIds( int startId, int step, int count )
    {
        int[] ints = new int[count];
        for ( int i = 0; i < count; i++ )
        {
            ints[i] = startId + i * step;
        }
        return ints;
    }

    private static PropertyRecord createSinglePropertyValue( PropertyStore store, int recordId, int keyId,
            Object value )
    {
        DynamicRecordAllocator stringAllocator = store.getStringStore();
        DynamicRecordAllocator arrayAllocator = store.getArrayStore();

        PropertyBlock block = new PropertyBlock();
        PropertyStore.encodeValue( block, keyId, value, stringAllocator, arrayAllocator );

        PropertyRecord record = new PropertyRecord( recordId );
        record.addPropertyBlock( block );
        record.setInUse( true );
        updateRecord( store, record );

        return record;
    }

    private static void createTwoPropertyValues( PropertyStore store, int recordId,
            int keyId1, Object value1, int keyId2, Object value2 ) throws IOException
    {
        DynamicRecordAllocator stringAllocator = store.getStringStore();
        DynamicRecordAllocator arrayAllocator = store.getArrayStore();

        PropertyBlock block1 = new PropertyBlock();
        PropertyStore.encodeValue( block1, keyId1, value1, stringAllocator, arrayAllocator );
        PropertyBlock block2 = new PropertyBlock();
        PropertyStore.encodeValue( block2, keyId2, value2, stringAllocator, arrayAllocator );

        PropertyRecord record = new PropertyRecord( recordId );
        record.addPropertyBlock( block1 );
        if ( block1.getSize() + block2.getSize() <= PropertyStore.DEFAULT_PAYLOAD_SIZE )
        {
            record.addPropertyBlock( block2 );
        }
        else
        {
            PropertyRecord nextRecord = new PropertyRecord( recordId + 1 );
            record.setNextProp( nextRecord.getId() );
            nextRecord.addPropertyBlock( block2 );
            nextRecord.setPrevProp( record.getId() );
            nextRecord.setInUse( true );
            updateRecord( store, nextRecord );
        }

        record.setInUse( true );
        updateRecord( store, record );
    }

    private static void markDynamicRecordNotInUse( int dynamicRecordIndex, int keyId, PropertyRecord record,
            PropertyStore store )
    {
        PropertyBlock propertyBlock = record.getPropertyBlock( keyId );
        store.ensureHeavy( propertyBlock );
        List<DynamicRecord> valueRecords = propertyBlock.getValueRecords();
        DynamicRecord dynamicRecord = valueRecords.get( dynamicRecordIndex );
        dynamicRecord.setInUse( false );
        updateRecord( store, record );
    }

    private static List<Object> asPropertyValuesList( StorePropertyCursor cursor )
    {
        List<Object> values = new ArrayList<>();
        while ( cursor.next() )
        {
            values.add( cursor.value() );
        }
        return values;
    }

    private static <T extends Abstract64BitRecord> void updateRecord( AbstractRecordStore<T> store, T record )
    {
        store.forceUpdateRecord( record );
        store.setHighestPossibleIdInUse( record.getLongId() );
    }
}
