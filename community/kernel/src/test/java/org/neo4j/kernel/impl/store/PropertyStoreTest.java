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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.internal.InOrderImpl;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.core.JumpingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class PropertyStoreTest
{
    @ClassRule
    public static PageCacheRule pageCacheRule = new PageCacheRule( false );

    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private EphemeralFileSystemAbstraction fileSystemAbstraction;
    private File path;

    @Before
    public void setup() throws IOException
    {
        fileSystemAbstraction = fsRule.get();
        path = new File( "/tmp/foobar" );

        fileSystemAbstraction.mkdir( path.getParentFile() );
        fileSystemAbstraction.create( path );
    }

    @Test
    public void shouldWriteOutTheDynamicChainBeforeUpdatingThePropertyRecord() throws IOException
    {
        // given
        PageCache pageCache = mock( PageCache.class );

        PagedFile storeFile = mock( PagedFile.class );
        when( pageCache.map( any( File.class ), anyInt() ) ).thenReturn( storeFile );

        DynamicStringStore stringPropertyStore = mock( DynamicStringStore.class );

        PageCursor cursor = mock( PageCursor.class );
        when( cursor.next() ).thenReturn( true );

        when( storeFile.io( anyInt(), anyInt() ) ).thenReturn( cursor );
        when( storeFile.pageSize() ).thenReturn( 8 );

        org.neo4j.kernel.configuration.Config config = mock( org.neo4j.kernel.configuration.Config.class );
        when( config.get( PropertyStore.Configuration.rebuild_idgenerators_fast ) ).thenReturn( true );


        PropertyStore store = new PropertyStore( path, config, new JumpingIdGeneratorFactory( 1 ), pageCache,
                fileSystemAbstraction, StringLogger
                .DEV_NULL, stringPropertyStore, mock( PropertyKeyTokenStore.class ), mock( DynamicArrayStore.class ),
                null, null );

        store.makeStoreOk();
        long l = store.nextId();

        PropertyRecord record = new PropertyRecord( l );
        record.setInUse( true );

        DynamicRecord dynamicRecord = dynamicRecord();
        PropertyBlock propertyBlock = propertyBlockWith( dynamicRecord );
        record.setPropertyBlock( propertyBlock );

        // when
        store.updateRecord( record );

        // then
        InOrderImpl inOrder = new InOrderImpl( Arrays.<Object>asList(stringPropertyStore, cursor) );

        inOrder.verify( stringPropertyStore ).updateRecord( dynamicRecord );

        inOrder.verify( cursor ).putByte( (byte) 0 );
        inOrder.verify( cursor, times( 2 ) ).putInt( -1 );
        inOrder.verify( cursor ).putLong( propertyBlock.getValueBlocks()[0] );
        inOrder.verify( cursor ).putLong( 0 );
    }

    private DynamicRecord dynamicRecord()
    {
        DynamicRecord dynamicRecord = new DynamicRecord( 42 );
        dynamicRecord.setType( PropertyType.STRING.intValue() );
        dynamicRecord.setCreated();
        return dynamicRecord;
    }

    private PropertyBlock propertyBlockWith( DynamicRecord dynamicRecord )
    {
        PropertyBlock propertyBlock = new PropertyBlock();

        PropertyKeyTokenRecord key = new PropertyKeyTokenRecord( 10 );
        propertyBlock.setSingleBlock( key.getId() | (((long) PropertyType.STRING.intValue()) << 24) | (dynamicRecord
                .getId() << 28) );
        propertyBlock.addValueRecord( dynamicRecord );


        return propertyBlock;
    }


}
