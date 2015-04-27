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
package org.neo4j.kernel.api.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.api.index.ProviderMeta.Record;
import org.neo4j.kernel.api.index.ProviderMeta.Snapshot;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.api.index.ProviderMeta.RECORD_SIZE;

public class ProviderMetaTest
{
    @Test
    public void shouldThrowInvalidRecordOnNonExistentRecord() throws Exception
    {
        // WHEN
        try
        {
            meta.getRecord( 10 );
            fail( "Should throw exception" );
        }
        catch ( InvalidRecordException e )
        {   // Expected
        }
    }

    @Test
    public void shouldUpdateRecordAndThenReadIt() throws Exception
    {
        // GIVEN
        long id = 3, value = 12345;
        Record record = new Record( id, value );
        record.setInUse( true );
        meta.updateRecord( record );

        // WHEN
        Record readRecord = meta.getRecord( id );

        // THEN
        assertEquals( record, readRecord );
        assertEquals( value, readRecord.getValue() );
        assertTrue( readRecord.inUse() );
    }

    @Test
    public void shouldUpdateNotInUseRecordAndThenGetInvalidRecordWhenReadingIt() throws Exception
    {
        // GIVEN
        long id = 3, value = 12345;
        Record record = new Record( id, value );
        record.setInUse( false );
        meta.updateRecord( record );

        // WHEN
        try
        {
            meta.getRecord( id );
            fail( "Should throw invalid record exception" );
        }
        catch ( InvalidRecordException e )
        {   // Expected
        }
    }

    @Test
    public void shouldDeferUpdateRecordToFileIfAnyActiveSnapshot() throws Exception
    {
        // GIVEN
        meta.updateRecord( new Record( 3, 5 ) );
        meta.force();

        // WHEN
        try ( Snapshot snapshot = meta.snapshot() )
        {
            meta.updateRecord( new Record( 3, 2 ) );

            // THEN
            meta.force();
            assertEquals( 2L, meta.getRecord( 3 ).getValue() );
            assertEquals( 5L, readRecordFromFile( 3 ) );
        }

        // AND WHEN
        meta.force();
        assertEquals( 2L, readRecordFromFile( 3 ) );
    }

    private long readRecordFromFile( long id ) throws IOException
    {
        try ( StoreChannel channel = fs.open( metaFile, "r" ) )
        {
            channel.position( RECORD_SIZE*id );
            ByteBuffer buffer = ByteBuffer.allocate( RECORD_SIZE );
            assertEquals( RECORD_SIZE, channel.read( buffer ) );
            buffer.flip();
            buffer.get();
            return buffer.getLong();
        }
    }

    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final LifeSupport life = new LifeSupport();
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private File metaFile;
    private ProviderMeta meta;

    @Before
    public void before()
    {
        metaFile = new File( directory.directory(), "meta" );
        meta = life.add( new ProviderMeta( fs, metaFile ) );
        life.start();
    }

    @After
    public void after()
    {
        life.shutdown();
    }
}
