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
package org.neo4j.kernel.impl.store.record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NeoStore.Position;
import org.neo4j.test.EphemeralFileSystemRule;

import static java.nio.ByteBuffer.wrap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.kernel.impl.store.NeoStore.TYPE_DESCRIPTOR;

public class NeoStoreUtilTest
{
    @Test
    public void shouldNotReadTrailerAsRecordData() throws Exception
    {
        // GIVEN
        long[] values = new long[] {1, 2, 3, 4, 5};
        NeoStoreUtil access = new NeoStoreUtil( neoStoreFile( fs.get(), true, values ), fs.get() );

        // WHEN
        for ( int i = 0; i < values.length; i++ )
        {
            assertEquals( values[i], access.getValue( Position.values()[i] ) );
        }

        // THEN
        try
        {
            access.getValue( Position.values()[values.length] );
            fail( "Shouldn't have read more records" );
        }
        catch ( IllegalStateException e )
        {   // OK
        }
    }

    @Test
    public void shouldReadAllRecordsEvenOnStoreWithoutTrailer() throws Exception
    {
        // GIVEN
        long[] values = new long[] {1, 2, 3, 4, 5};
        NeoStoreUtil access = new NeoStoreUtil( neoStoreFile( fs.get(), false, values ), fs.get() );

        // WHEN
        for ( int i = 0; i < values.length; i++ )
        {
            assertEquals( values[i], access.getValue( Position.values()[i] ) );
        }

        // THEN
        try
        {
            access.getValue( Position.values()[values.length] );
            fail( "Shouldn't have read more records" );
        }
        catch ( IllegalStateException e )
        {   // OK
        }
    }

    private File neoStoreFile( FileSystemAbstraction fs, boolean addVersionTrailer, long... recordValues )
            throws IOException
    {
        File dir = new File( "dir" );
        fs.mkdirs( dir );
        try ( StoreChannel channel = fs.create( new File( dir, NeoStore.DEFAULT_NAME ) ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( NeoStore.RECORD_SIZE );
            for ( long recordValue : recordValues )
            {
                buffer.clear();
                buffer.put( Record.IN_USE.byteValue() );
                buffer.putLong( recordValue );
                buffer.flip();
                channel.write( buffer );
            }

            if ( addVersionTrailer )
            {
                channel.write( wrap( encode( NeoStore.buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR ) ) ) );
            }
        }
        return dir;
    }

    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
