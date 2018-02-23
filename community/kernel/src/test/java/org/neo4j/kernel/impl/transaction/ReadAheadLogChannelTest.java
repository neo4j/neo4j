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
package org.neo4j.kernel.impl.transaction;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Resource;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;

@EnableRuleMigrationSupport
@ExtendWith( TestDirectoryExtension.class )
public class ReadAheadLogChannelTest
{

    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Resource
    public TestDirectory directory;

    @Test
    public void shouldReadFromSingleChannel() throws Exception
    {
        // GIVEN
        File file = file( 0 );
        final byte byteValue = (byte) 5;
        final short shortValue = (short) 56;
        final int intValue = 32145;
        final long longValue = 5689456895869L;
        final float floatValue = 12.12345f;
        final double doubleValue = 3548.45748D;
        final byte[] byteArrayValue = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9};
        writeSomeData( file, element ->
        {
            element.put( byteValue );
            element.putShort( shortValue );
            element.putInt( intValue );
            element.putLong( longValue );
            element.putFloat( floatValue );
            element.putDouble( doubleValue );
            element.put( byteArrayValue );
            return true;
        } );

        StoreChannel storeChannel = fileSystemRule.get().open( file, OpenMode.READ );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, -1 /* ignored */, (byte) -1 /* ignored */ );
        try ( ReadAheadLogChannel channel = new ReadAheadLogChannel( versionedStoreChannel, NO_MORE_CHANNELS, 16 ) )
        {
            // THEN
            assertEquals( byteValue, channel.get() );
            assertEquals( shortValue, channel.getShort() );
            assertEquals( intValue, channel.getInt() );
            assertEquals( longValue, channel.getLong() );
            assertEquals( channel.getFloat(), 0.1f, floatValue );
            assertEquals( channel.getDouble(), 0.1d, doubleValue );

            byte[] bytes = new byte[byteArrayValue.length];
            channel.get( bytes, byteArrayValue.length );
            assertArrayEquals( byteArrayValue, bytes );
        }
    }

    @Test
    public void shouldReadFromMultipleChannels() throws Exception
    {
        // GIVEN
        writeSomeData( file( 0 ), element ->
        {
            for ( int i = 0; i < 10; i++ )
            {
                element.putLong( i );
            }
            return true;
        } );
        writeSomeData( file( 1 ), element ->
        {
            for ( int i = 10; i < 20; i++ )
            {
                element.putLong( i );
            }
            return true;
        } );

        StoreChannel storeChannel = fileSystemRule.get().open( file( 0 ), OpenMode.READ );
        PhysicalLogVersionedStoreChannel versionedStoreChannel =
                new PhysicalLogVersionedStoreChannel( storeChannel, -1 /* ignored */, (byte) -1 /* ignored */ );
        try ( ReadAheadLogChannel channel = new ReadAheadLogChannel( versionedStoreChannel, new LogVersionBridge()
        {
            private boolean returned;

            @Override
            public LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
            {
                if ( !returned )
                {
                    returned = true;
                    channel.close();
                    return new PhysicalLogVersionedStoreChannel( fileSystemRule.get().open( file( 1 ), OpenMode.READ ),
                            -1 /* ignored */, (byte) -1 /* ignored */ );
                }
                return channel;
            }
        }, 10 ) )
        {
            // THEN
            for ( long i = 0; i < 20; i++ )
            {
                assertEquals( i, channel.getLong() );
            }
        }
    }

    private void writeSomeData( File file, Visitor<ByteBuffer, IOException> visitor ) throws IOException
    {
        try ( StoreChannel channel = fileSystemRule.get().open( file, OpenMode.READ_WRITE ) )
        {
            ByteBuffer buffer = ByteBuffer.allocate( 1024 );
            visitor.visit( buffer );
            buffer.flip();
            channel.write( buffer );
        }
    }

    private File file( int index )
    {
        return new File( directory.directory(), "" + index );
    }
}
