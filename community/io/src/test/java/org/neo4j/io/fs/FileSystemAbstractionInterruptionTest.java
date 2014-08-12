/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.fs;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class FileSystemAbstractionInterruptionTest
{
    private static final Factory<FileSystemAbstraction> ephemeral = new Factory<FileSystemAbstraction>()
    {
        @Override
        public FileSystemAbstraction newInstance()
        {
            return new EphemeralFileSystemAbstraction();
        }
    };

    private static final Factory<FileSystemAbstraction> real = new Factory<FileSystemAbstraction>()
    {
        @Override
        public FileSystemAbstraction newInstance()
        {
            return new DefaultFileSystemAbstraction();
        }
    };

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> dataPoints()
    {
        return Arrays.asList(new Object[][]{
                { "ephemeral", ephemeral },
                { "real", real }
        });
    }

    @Rule
    public final TargetDirectory.TestDirectory testdir =
            TargetDirectory.testDirForTest( FileSystemAbstractionInterruptionTest.class );

    private FileSystemAbstraction fs;
    private File file;

    public FileSystemAbstractionInterruptionTest( String name, Factory<FileSystemAbstraction> factory )
    {
        fs = factory.newInstance();
    }

    @Before
    public void interruptPriorToCall()
    {
        Thread.currentThread().interrupt();
    }

    @Before
    public void createWorkingDirectoryAndTestFile() throws IOException
    {
        fs.mkdirs( testdir.directory() );
        file = testdir.file( "a" );
        fs.create( file ).close();
    }

    @After
    public void verifyInterruptionFlagStillRaised()
    {
        assertTrue( Thread.interrupted() );
    }

    @Test
    public void fs_openClose() throws IOException
    {
        fs.open( file, "rw" ).close();
    }

    @Test
    public void fs_tryLock() throws IOException
    {
        fs.tryLock( file, fs.open( file, "rw" ) ).release();
    }

    @Test
    public void ch_tryLock() throws IOException
    {
        fs.open( file, "rw" ).tryLock().release();
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_setPosition() throws IOException
    {
        fs.open( file, "rw" ).position( 0 );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_getPosition() throws IOException
    {
        fs.open( file, "rw" ).position();
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_truncate() throws IOException
    {
        fs.open( file, "rw" ).truncate( 0 );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_force() throws IOException
    {
        fs.open( file, "rw" ).force( true );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_writeAll_ByteBuffer() throws IOException
    {
        fs.open( file, "rw" ).writeAll( ByteBuffer.allocate( 1 ) );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_writeAll_ByteBuffer_position() throws IOException
    {
        fs.open( file, "rw" ).writeAll( ByteBuffer.allocate( 1 ), 1 );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_write_ByteBuffer_position() throws IOException
    {
        fs.open( file, "rw" ).write( ByteBuffer.allocate( 1 ), 1 );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_map() throws IOException
    {
        fs.open( file, "rw" ).map( FileChannel.MapMode.READ_ONLY, 0, 10 );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_read_ByteBuffer() throws IOException
    {
        fs.open( file, "rw" ).read( ByteBuffer.allocate( 1 ) );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_write_ByteBuffer() throws IOException
    {
        fs.open( file, "rw" ).write( ByteBuffer.allocate( 1 ) );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_size() throws IOException
    {
        fs.open( file, "rw" ).size();
    }

    @Test
    public void ch_isOpen() throws IOException
    {
        fs.open( file, "rw" ).isOpen();
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_write_ByteBuffers_offset_length() throws IOException
    {
        fs.open( file, "rw" ).write( new ByteBuffer[]{ ByteBuffer.allocate( 1 ) }, 0, 1 );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_write_ByteBuffers() throws IOException
    {
        fs.open( file, "rw" ).write( new ByteBuffer[]{ ByteBuffer.allocate( 1 ) } );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_read_ByteBuffers_offset_length() throws IOException
    {
        fs.open( file, "rw" ).read( new ByteBuffer[]{ ByteBuffer.allocate( 1 ) }, 0, 1 );
    }

    @Test(expected = ClosedByInterruptException.class)
    public void ch_read_ByteBuffers() throws IOException
    {
        fs.open( file, "rw" ).read( new ByteBuffer[]{ ByteBuffer.allocate( 1 ) } );
    }
}
