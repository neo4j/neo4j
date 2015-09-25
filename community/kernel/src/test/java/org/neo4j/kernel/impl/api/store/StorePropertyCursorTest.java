/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.function.Consumers;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.StoreFactory.PROPERTY_STORE_NAME;

@RunWith( Enclosed.class )
public class StorePropertyCursorTest
{
    private static final List<Object[]> PARAMETERS = Arrays.asList(
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
            StorePropertyCursor storePropertyCursor = new StorePropertyCursor( propertyStore, cache );

            // when
            storePropertyCursor.close();

            // then
            verify( cache, times( 1 ) ).accept( storePropertyCursor );
        }

        @Test
        public void shouldThrowIfTheRecordIsNotInUse() throws Throwable
        {
            // given
            int recordId = 42;
            PageCursor pageCursor = mock( PageCursor.class );
            when( propertyStore.newReadCursor( recordId ) ).thenReturn( pageCursor );
            when( pageCursor.shouldRetry() ).thenReturn( false );

            StorePropertyCursor storePropertyCursor = new StorePropertyCursor( propertyStore, cache );

            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId ) )
            {
                // when
                cursor.next();
                fail();
            }
            catch ( NotFoundException ex )
            {
                // then
                assertEquals( "Property record with id " + recordId + " not in use", ex.getMessage() );
            }
        }
    }

    @RunWith( Parameterized.class )
    public static class SingleValueProperties
    {
        @Rule
        public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
        @Rule
        public PageCacheRule pageCacheRule = new PageCacheRule( true );

        private final Consumer<StorePropertyCursor> cache = Consumers.noop();

        private PropertyStore propertyStore;

        @Before
        public void setup() throws IOException
        {
            EphemeralFileSystemAbstraction fs = fsRule.get();
            PageCache pageCache = pageCacheRule.getPageCache( fs );
            LogProvider log = NullLogProvider.getInstance();
            Monitors monitors = new Monitors();

            File storeDir = new File( "store" );
            fs.mkdirs( storeDir );
            StoreFactory storeFactory = new StoreFactory( fs, storeDir, pageCache, log, monitors );
            storeFactory.createPropertyStore();

            propertyStore = storeFactory.newPropertyStore( storeFactory.storeFileName( PROPERTY_STORE_NAME ) );
        }

        @After
        public void tearDown()
        {
            propertyStore.close();
        }

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

            StorePropertyCursor storePropertyCursor = new StorePropertyCursor( propertyStore, cache );

            // when
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId ) )
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
    public static class TwoValueProperties
    {
        @Rule
        public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
        @Rule
        public PageCacheRule pageCacheRule = new PageCacheRule( true );

        private final Consumer<StorePropertyCursor> cache = Consumers.noop();

        private PropertyStore propertyStore;

        @Before
        public void setup() throws IOException
        {
            EphemeralFileSystemAbstraction fs = fsRule.get();
            PageCache pageCache = pageCacheRule.getPageCache( fs );
            LogProvider log = NullLogProvider.getInstance();
            Monitors monitors = new Monitors();

            File storeDir = new File( "store" );
            fs.mkdirs( storeDir );
            StoreFactory storeFactory = new StoreFactory( fs, storeDir, pageCache, log, monitors );
            storeFactory.createPropertyStore();

            propertyStore = storeFactory.newPropertyStore( storeFactory.storeFileName( PROPERTY_STORE_NAME ) );
        }

        @After
        public void tearDown()
        {
            propertyStore.close();
        }

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

            StorePropertyCursor storePropertyCursor = new StorePropertyCursor( propertyStore, cache );

            // when
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId ) )
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

            StorePropertyCursor storePropertyCursor = new StorePropertyCursor( propertyStore, cache );

            // when
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId ) )
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
    public static class CursorReuse
    {
        @Rule
        public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
        @Rule
        public PageCacheRule pageCacheRule = new PageCacheRule( true );

        private final Consumer<StorePropertyCursor> cache = Consumers.noop();

        private PropertyStore propertyStore;

        @Before
        public void setup() throws IOException
        {
            EphemeralFileSystemAbstraction fs = fsRule.get();
            PageCache pageCache = pageCacheRule.getPageCache( fs );
            LogProvider log = NullLogProvider.getInstance();
            Monitors monitors = new Monitors();

            File storeDir = new File( "store" );
            fs.mkdirs( storeDir );
            StoreFactory storeFactory = new StoreFactory( fs, storeDir, pageCache, log, monitors );
            storeFactory.createPropertyStore();

            propertyStore = storeFactory.newPropertyStore( storeFactory.storeFileName( PROPERTY_STORE_NAME ) );
        }

        @After
        public void tearDown()
        {
            propertyStore.close();
        }

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

            StorePropertyCursor storePropertyCursor = new StorePropertyCursor( propertyStore, cache );

            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId ) )
            {
                assertTrue( cursor.next() );
                PropertyItem item = cursor.get();
                assertEquals( keyId, item.propertyKeyId() );
                assertEqualValues( expectedValue, item );
                assertFalse( cursor.next() );
            }

            // when using it
            try ( Cursor<PropertyItem> cursor = storePropertyCursor.init( recordId ) )
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

    private static void createSinglePropertyValue( PropertyStore store, int recordId, int keyId, Object value )
            throws IOException
    {
        DynamicRecordAllocator stringAllocator = store.getStringStore();
        DynamicRecordAllocator arrayAllocator = store.getArrayStore();

        PropertyBlock block = new PropertyBlock();
        PropertyStore.encodeValue( block, keyId, value, stringAllocator, arrayAllocator );


        PropertyRecord record = new PropertyRecord( recordId );
        record.addPropertyBlock( block );
        record.setInUse( true );
        store.updateRecord( record );
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
            store.updateRecord( nextRecord );
        }

        record.setInUse( true );
        store.updateRecord( record );
    }
}
