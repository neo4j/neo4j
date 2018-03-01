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
package org.neo4j.kernel.api.impl.index.storage.paged;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.store.RandomAccessInput;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.BiFunction;

import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.function.ThrowingBiFunction;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

public class PagedIndexInputTest
{
    public TestDirectory tmpDir = TestDirectory.testDirectory( getClass() );
    public FileSystemRule fs = new EphemeralFileSystemRule();
    public PageCacheRule pageCache = new PageCacheRule();

    @Rule
    public RuleChain rules = RuleChain
            .outerRule( tmpDir )
            .around( fs )
            .around( pageCache );

    private PageCache pc;
    private Directory dir;

    @Test
    public void shouldReadAndWriteByte() throws Exception
    {
        test( (byte) 7, IndexOutput::writeByte, IndexInput::readByte );
        test( (byte) 0, IndexOutput::writeByte, IndexInput::readByte );
        test( (byte) -1, IndexOutput::writeByte, IndexInput::readByte );
        test( Byte.MAX_VALUE, IndexOutput::writeByte, IndexInput::readByte );
        test( Byte.MIN_VALUE, IndexOutput::writeByte, IndexInput::readByte );
    }

    @Test
    public void shouldReadAndWriteShort() throws Exception
    {
        test( (short) 7, IndexOutput::writeShort, IndexInput::readShort );
        test( (short) 0, IndexOutput::writeShort, IndexInput::readShort );
        test( (short) -1, IndexOutput::writeShort, IndexInput::readShort );
        test( Short.MAX_VALUE, IndexOutput::writeShort, IndexInput::readShort );
        test( Short.MIN_VALUE, IndexOutput::writeShort, IndexInput::readShort );
    }

    @Test
    public void shouldReadAndWriteInteger() throws Exception
    {
        test( 7, IndexOutput::writeInt, IndexInput::readInt );
        test( 0, IndexOutput::writeInt, IndexInput::readInt );
        test( -1, IndexOutput::writeInt, IndexInput::readInt );
        test( Integer.MAX_VALUE, IndexOutput::writeInt, IndexInput::readInt );
        test( Integer.MIN_VALUE, IndexOutput::writeInt, IndexInput::readInt );
    }

    @Test
    public void shouldReadAndWriteLong() throws Exception
    {
        test( 7L, IndexOutput::writeLong, IndexInput::readLong );
        test( 0L, IndexOutput::writeLong, IndexInput::readLong );
        test( -1L, IndexOutput::writeLong, IndexInput::readLong );
        test( Long.MAX_VALUE, IndexOutput::writeLong, IndexInput::readLong );
        test( Long.MIN_VALUE, IndexOutput::writeLong, IndexInput::readLong );
    }

    @Test
    public void shouldReadAndWriteBytes() throws Exception
    {
        testByteArray( new byte[]{1, 3, 3, -1} );
        testByteArray( new byte[]{(byte) 8} );

        byte[] bigAsPage = new byte[pc.pageSize()];
        Arrays.fill( bigAsPage, (byte) -1 );
        testByteArray( bigAsPage );

        byte[] bigAsPagePlusOne = new byte[pc.pageSize() + 1];
        Arrays.fill( bigAsPagePlusOne, (byte) -1 );
        testByteArray( bigAsPagePlusOne );
    }

    @Test
    public void shouldRandomAccessReadAndWriteByte() throws Exception
    {
        testRandomAccess( (byte) 7, IndexOutput::writeByte,
                RandomAccessInput::readByte );
        testRandomAccess( (byte) 0, IndexOutput::writeByte,
                RandomAccessInput::readByte );
        testRandomAccess( (byte) -1, IndexOutput::writeByte,
                RandomAccessInput::readByte );
        testRandomAccess( Byte.MAX_VALUE, IndexOutput::writeByte,
                RandomAccessInput::readByte );
        testRandomAccess( Byte.MIN_VALUE, IndexOutput::writeByte,
                RandomAccessInput::readByte );
    }

    @Test
    public void shouldRandomAccessReadAndWriteShort() throws Exception
    {
        testRandomAccess( (short) 7, IndexOutput::writeShort,
                RandomAccessInput::readShort );
        testRandomAccess( (short) 0, IndexOutput::writeShort,
                RandomAccessInput::readShort );
        testRandomAccess( (short) -1, IndexOutput::writeShort,
                RandomAccessInput::readShort );
        testRandomAccess( Short.MAX_VALUE, IndexOutput::writeShort,
                RandomAccessInput::readShort );
        testRandomAccess( Short.MIN_VALUE, IndexOutput::writeShort,
                RandomAccessInput::readShort );
    }

    @Test
    public void shouldRandomAccessReadAndWriteInt() throws Exception
    {
        testRandomAccess( 7, IndexOutput::writeInt,
                RandomAccessInput::readInt );
        testRandomAccess( 0, IndexOutput::writeInt,
                RandomAccessInput::readInt );
        testRandomAccess( -1, IndexOutput::writeInt,
                RandomAccessInput::readInt );
        testRandomAccess( Integer.MAX_VALUE, IndexOutput::writeInt,
                RandomAccessInput::readInt );
        testRandomAccess( Integer.MIN_VALUE, IndexOutput::writeInt,
                RandomAccessInput::readInt );
    }

    @Test
    public void shouldRandomAccessReadAndWriteLong() throws Exception
    {
        testRandomAccess( 7L, IndexOutput::writeLong,
                RandomAccessInput::readLong );
        testRandomAccess( 0L, IndexOutput::writeLong,
                RandomAccessInput::readLong );
        testRandomAccess( -1L, IndexOutput::writeLong,
                RandomAccessInput::readLong );
        testRandomAccess( Long.MAX_VALUE, IndexOutput::writeLong,
                RandomAccessInput::readLong );
        testRandomAccess( Long.MIN_VALUE, IndexOutput::writeLong,
                RandomAccessInput::readLong );
    }

    private <T> void testRandomAccess( T val,
            ThrowingBiConsumer<IndexOutput,T,IOException> write,
            ThrowingBiFunction<RandomAccessInput,Long,T,IOException> read )
            throws IOException
    {
        testRandomAccess( val, 0, read, write );
        testRandomAccess( val, 1, read, write );
        testRandomAccess( val, 3, read, write );
        testRandomAccess( val, 4, read, write );
        testRandomAccess( val, 7, read, write );
        testRandomAccess( val, 8, read, write );
        testRandomAccess( val, 9, read, write );
        testRandomAccess( val, pc.pageSize() - 8, read, write );
        testRandomAccess( val, pc.pageSize() - 7, read, write );
        testRandomAccess( val, pc.pageSize() - 5, read, write );
        testRandomAccess( val, pc.pageSize() - 4, read, write );
        testRandomAccess( val, pc.pageSize() - 3, read, write );
        testRandomAccess( val, pc.pageSize() - 2, read, write );
        testRandomAccess( val, pc.pageSize() - 1, read, write );
        testRandomAccess( val, pc.pageSize(), read, write );
        testRandomAccess( val, pc.pageSize() + 8, read, write );
        testRandomAccess( val, pc.pageSize() + 7, read, write );
        testRandomAccess( val, pc.pageSize() + 5, read, write );
        testRandomAccess( val, pc.pageSize() + 4, read, write );
        testRandomAccess( val, pc.pageSize() + 3, read, write );
        testRandomAccess( val, pc.pageSize() + 2, read, write );
        testRandomAccess( val, pc.pageSize() + 1, read, write );
    }

    @Test
    public void shouldGiveCorrectPosition() throws Exception
    {
        String fileName = testFileName( "" );
        int fileLen = 17;

        // Create a fileLen long file..
        try ( IndexOutput out = dir.createOutput( fileName,
                IOContext.DEFAULT ) )
        {
            out.writeBytes( new byte[fileLen], fileLen );
        }

        // And then check that file pointer gives us the correct position at each position
        long currentPosition = 0;
        try ( IndexInput in = dir.openInput( fileName, IOContext.READ ) )
        {
            // Read until EOF
            try
            {
                while ( true )
                {
                    long actual = in.getFilePointer();
                    assert currentPosition == actual :
                            String.format( "Expected position to be %d, got %d",
                                    currentPosition, actual );
                    assert currentPosition <= fileLen : String.format(
                            "Expected position to be less than file length of " +
                                    "%d, got %d", fileLen, currentPosition );
                    in.readByte();
                    currentPosition++;
                }
            }
            catch ( EOFException e )
            {
                // Expected
            }
            long actual = in.getFilePointer();
            assert actual == fileLen :
                    String.format( "Expected position to be %s, got %s",
                            fileLen, actual );
        }
    }

    private <T> void testRandomAccess( T val, long pos,
            ThrowingBiFunction<RandomAccessInput,Long,T,IOException> read,
            ThrowingBiConsumer<IndexOutput,T,IOException> write )
            throws IOException
    {
        long len = pos + valueSize( val, write );
        ThrowingBiConsumer<IndexOutput,T,IOException> writeWrap = ( o, v ) ->
        {
            o.writeBytes( new byte[(int) pos], (int) pos );
            write.accept( o, v );
        };
        ThrowingFunction<IndexInput,T,IOException> readWrap =
                i -> read.apply( (RandomAccessInput) i, pos );

        // Test regular input
        test( val, writeWrap, readWrap, Object::equals, 0, 0, len );

        // Test sliced input
        test( val, writeWrap, readWrap, Object::equals, 0, 1, len );
        test( val, writeWrap, readWrap, Object::equals, 0, pc.pageSize() - 1,
                len );
        test( val, writeWrap, readWrap, Object::equals, 0, pc.pageSize(), len );
        test( val, writeWrap, readWrap, Object::equals, 0, pc.pageSize(), len );
    }

    private <T> int valueSize( T val,
            ThrowingBiConsumer<IndexOutput,T,IOException> write )
            throws IOException
    {
        RAMOutputStream t = new RAMOutputStream();
        write.accept( t, val );
        return (int) t.getFilePointer();
    }

    private void testByteArray( byte[] val ) throws IOException
    {
        testByteArray( val, 0 );
        testByteArray( val, 1 );
        testByteArray( val, val.length );
        testByteArray( val, val.length + 1 );
        testByteArray( val, pc.pageSize() - 1 );
        testByteArray( val, pc.pageSize() );
        testByteArray( val, pc.pageSize() + 1 );
        if ( val.length > 0 )
        {
            testByteArray( val, val.length - 1 );
        }
    }

    private void testByteArray( byte[] val, int readOffset ) throws IOException
    {
        ThrowingBiConsumer<IndexOutput,byte[],IOException> write =
                ( o, v ) -> o.writeBytes( v, v.length );
        ThrowingFunction<IndexInput,byte[],IOException> read = i ->
        {
            byte[] actual = new byte[val.length + readOffset];
            i.readBytes( actual, readOffset, val.length );
            return Arrays.copyOfRange( actual, readOffset,
                    readOffset + val.length );
        };
        test( val, write, read, Arrays::equals, val.length );
    }

    @Before
    public void setup() throws IOException
    {
        pc = this.pageCache.getPageCache( fs );
        // Just in case someone wants standard file system, use actual tmp dir
        Path workDir = tmpDir.directory().toPath();
        fs.mkdirs( workDir.toFile() );
        dir = new PagedDirectory( workDir, pc );
    }

    private <T> void test( T val,
            ThrowingBiConsumer<IndexOutput,T,IOException> write,
            ThrowingFunction<IndexInput,T,IOException> read ) throws IOException
    {
        test( val, write, read, Object::equals, valueSize( val, write ) );
    }

    private <T> void test( T val,
            ThrowingBiConsumer<IndexOutput,T,IOException> write,
            ThrowingFunction<IndexInput,T,IOException> read,
            BiFunction<T,T,Boolean> equals, long valueSize ) throws IOException
    {
        // Test multiple offsets at the beginning on a page
        for ( int offset = 0; offset < 3; offset++ )
        {
            testRegularAndSliced( val, write, read, equals, offset, valueSize );
        }

        // Test multiple offsets at page boundary
        for ( int offset = pc.pageSize() - 16; offset < pc.pageSize() + 1;
                offset++ )
        {
            testRegularAndSliced( val, write, read, equals, offset, valueSize );
        }
    }

    private <T> void testRegularAndSliced( T val,
            ThrowingBiConsumer<IndexOutput,T,IOException> write,
            ThrowingFunction<IndexInput,T,IOException> read,
            BiFunction<T,T,Boolean> equals, int offset, long valueSize )
            throws IOException
    {
        // Test regular input
        test( val, write, read, equals, offset, 0, 0 );

        // Test reading the data back via sliced input
        test( val, write, read, equals, offset, 0, offset + valueSize );
        test( val, write, read, equals, offset, 1, offset + valueSize );
        test( val, write, read, equals, offset, pc.pageSize() - 1,
                offset + valueSize );
        test( val, write, read, equals, offset, pc.pageSize(),
                offset + valueSize );
    }

    private <T> void test( T val,
            ThrowingBiConsumer<IndexOutput,T,IOException> write,
            ThrowingFunction<IndexInput,T,IOException> read,
            BiFunction<T,T,Boolean> equals, int offset, long sliceStart,
            long sliceLength ) throws IOException
    {
        // Test using seek to position the reader
        test( val, write, read, equals, offset, sliceStart, sliceLength,
                IndexInput::seek );
        // Test using skipBytes to position the reader
        test( val, write, read, equals, offset, sliceStart, sliceLength,
                IndexInput::skipBytes );
    }

    private <T> void test( T val,
            ThrowingBiConsumer<IndexOutput,T,IOException> write,
            ThrowingFunction<IndexInput,T,IOException> read,
            BiFunction<T,T,Boolean> equals, int offset, long sliceStart,
            long sliceLength,
            ThrowingBiConsumer<IndexInput,Integer,IOException> skipMethod )
            throws IOException
    {
        String fileName = testFileName(
                String.format( "o=%d,s=%d,l=%d", offset, sliceStart,
                        sliceLength ) );

        // Write the value
        try ( IndexOutput out = dir.createOutput( fileName,
                IOContext.DEFAULT ) )
        {
            out.writeBytes( new byte[(int) (offset + sliceStart)],
                    (int) (offset + sliceStart) );
            write.accept( out, val );
        }

        // Read it back
        try ( IndexInput in = dir.openInput( fileName, IOContext.READ ) )
        {
            IndexInput target = in;
            try
            {
                if ( sliceStart > 0 || sliceLength > 0 )
                {
                    target = in.slice(
                            String.format( "slice s=%d,l=%d", sliceStart,
                                    sliceLength ), sliceStart, sliceLength );
                }
                skipMethod.accept( target, offset );
                T actual = read.apply( target );

                String expected =
                        val instanceof byte[] ? Arrays.toString( (byte[]) val )
                                              : val.toString();
                String found = actual instanceof byte[] ? Arrays.toString(
                        (byte[]) actual ) : actual.toString();
                assert equals.apply( val, actual ) :
                        String.format( "Expected %s, got %s, when offset by %d",
                                expected, found, offset );
            }
            finally
            {
                if ( target != in )
                {
                    target.close();
                }
            }
        }
    }

    private static String testFileName( String suffix )
    {
        // Just to create a file name that's at least marginally helpful
        StringBuilder name = new StringBuilder();
        StackTraceElement testMethod =
                Thread.currentThread().getStackTrace()[4];
        name.append( testMethod.getMethodName() ).append( "L" ).append(
                testMethod.getLineNumber() ).append( "." );
        return name.append( suffix ).toString();
    }
}
