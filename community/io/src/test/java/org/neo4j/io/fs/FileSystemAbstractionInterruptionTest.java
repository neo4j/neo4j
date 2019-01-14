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
package org.neo4j.io.fs;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class FileSystemAbstractionInterruptionTest
{
    private static final Factory<FileSystemAbstraction> ephemeral = EphemeralFileSystemAbstraction::new;
    private static final Factory<FileSystemAbstraction> real = DefaultFileSystemAbstraction::new;

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> dataPoints()
    {
        return Arrays.asList( new Object[][]{{"ephemeral", ephemeral}, {"real", real}} );
    }

    @Rule
    public final TestDirectory testdir = TestDirectory.testDirectory();

    private FileSystemAbstraction fs;
    private File file;

    public FileSystemAbstractionInterruptionTest( @SuppressWarnings( "UnusedParameters" ) String name,
            Factory<FileSystemAbstraction> factory )
    {
        fs = factory.newInstance();
    }

    @Before
    public void createWorkingDirectoryAndTestFile() throws IOException
    {
        Thread.interrupted();
        fs.mkdirs( testdir.directory() );
        file = testdir.file( "a" );
        fs.create( file ).close();
        channel = null;
        channelShouldBeClosed = false;
        Thread.currentThread().interrupt();
    }

    private StoreChannel channel;
    private boolean channelShouldBeClosed;

    @After
    public void verifyInterruptionAndChannelState() throws IOException
    {
        assertTrue( Thread.interrupted() );
        assertThat( "channelShouldBeClosed? " + channelShouldBeClosed,
                channel.isOpen(),
                is( !channelShouldBeClosed ) );

        if ( channelShouldBeClosed )
        {
            try
            {
                channel.force( true );
                fail( "Operating on a closed channel should fail" );
            }
            catch ( ClosedChannelException expected )
            {
                // This is good. What we expect to see.
            }
        }
        channel.close();
        fs.close();
    }

    private StoreChannel chan( boolean channelShouldBeClosed ) throws IOException
    {
        this.channelShouldBeClosed = channelShouldBeClosed;
        channel = fs.open( file, OpenMode.READ_WRITE );
        return channel;
    }

    @Test
    public void fs_openClose() throws IOException
    {
        chan( true ).close();
    }

    @Test
    public void ch_tryLock() throws IOException
    {
        chan( false ).tryLock().release();
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_setPosition() throws IOException
    {
        chan( true ).position( 0 );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_getPosition() throws IOException
    {
        chan( true ).position();
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_truncate() throws IOException
    {
        chan( true ).truncate( 0 );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_force() throws IOException
    {
        chan( true ).force( true );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_writeAll_ByteBuffer() throws IOException
    {
        chan( true ).writeAll( ByteBuffer.allocate( 1 ) );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_writeAll_ByteBuffer_position() throws IOException
    {
        chan( true ).writeAll( ByteBuffer.allocate( 1 ), 1 );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_read_ByteBuffer() throws IOException
    {
        chan( true ).read( ByteBuffer.allocate( 1 ) );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_write_ByteBuffer() throws IOException
    {
        chan( true ).write( ByteBuffer.allocate( 1 ) );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_size() throws IOException
    {
        chan( true ).size();
    }

    @Test
    public void ch_isOpen() throws IOException
    {
        chan( false ).isOpen();
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_write_ByteBuffers_offset_length() throws IOException
    {
        chan( true ).write( new ByteBuffer[]{ByteBuffer.allocate( 1 )}, 0, 1 );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_write_ByteBuffers() throws IOException
    {
        chan( true ).write( new ByteBuffer[]{ByteBuffer.allocate( 1 )} );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_read_ByteBuffers_offset_length() throws IOException
    {
        chan( true ).read( new ByteBuffer[]{ByteBuffer.allocate( 1 )}, 0, 1 );
    }

    @Test( expected = ClosedByInterruptException.class )
    public void ch_read_ByteBuffers() throws IOException
    {
        chan( true ).read( new ByteBuffer[]{ByteBuffer.allocate( 1 )} );
    }
}
