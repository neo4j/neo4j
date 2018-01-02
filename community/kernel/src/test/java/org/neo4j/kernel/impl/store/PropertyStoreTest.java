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
package org.neo4j.kernel.impl.store;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.JumpingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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
    }

    @Test
    public void shouldWriteOutTheDynamicChainBeforeUpdatingThePropertyRecord() throws IOException
    {
        // given
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemAbstraction );
        Config config = new Config();
        config.setProperty( PropertyStore.Configuration.rebuild_idgenerators_fast.name(), "true" );

        DynamicStringStore stringPropertyStore = mock( DynamicStringStore.class );

        final PropertyStore store = new PropertyStore( path, config, new JumpingIdGeneratorFactory( 1 ), pageCache,
                NullLogProvider.getInstance(), stringPropertyStore,
                mock( PropertyKeyTokenStore.class ), mock( DynamicArrayStore.class )  );
        store.initialise( true );

        try
        {
            store.makeStoreOk();
            final long propertyRecordId = store.nextId();

            PropertyRecord record = new PropertyRecord( propertyRecordId );
            record.setInUse( true );

            DynamicRecord dynamicRecord = dynamicRecord();
            PropertyBlock propertyBlock = propertyBlockWith( dynamicRecord );
            record.setPropertyBlock( propertyBlock );

            doAnswer( new Answer()
            {
                @Override
                public Object answer( InvocationOnMock invocation ) throws Throwable
                {
                    PropertyRecord recordBeforeWrite = store.forceGetRecord( propertyRecordId );
                    assertFalse( recordBeforeWrite.inUse() );
                    return null;
                }
            } ).when( stringPropertyStore ).updateRecord( dynamicRecord );

            // when
            store.updateRecord( record );

            // then verify that our mocked method above, with the assert, was actually called
            verify( stringPropertyStore ).updateRecord( dynamicRecord );
        }
        finally
        {
            store.close();
        }
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
