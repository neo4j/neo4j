/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandlerContext;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.copyOfRange;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.causalclustering.catchup.storecopy.FileChunk.MAX_SIZE;

public class FileSenderTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fsRule.get());

    private final ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
    private final FileSystemAbstraction fs = fsRule.get();
    private final Random random = new Random();

    @Test
    public void sendEmptyFile() throws IOException
    {
        // given
        FileSender fileSender = new FileSender( fs, ctx );
        File smallFile = testDirectory.file( "smallFile" );
        fs.create( smallFile ).close();

        // when
        fileSender.sendFile( smallFile );

        // then
        verify( ctx ).writeAndFlush( ResponseMessageType.FILE );
        verify( ctx ).writeAndFlush( new FileHeader( smallFile.getName() ) );
        verify( ctx ).writeAndFlush( FileChunk.create( new byte[0], true ) );
        verifyNoMoreInteractions( ctx );
    }

    @Test
    public void sendSmallFile() throws IOException
    {
        // given
        byte[] bytes = new byte[10];
        random.nextBytes( bytes );
        FileSender fileSender = new FileSender( fs, ctx );
        File smallFile = testDirectory.file( "smallFile" );
        try( StoreChannel storeChannel = fs.create( smallFile ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }

        // when
        fileSender.sendFile( smallFile );

        // then
        verify( ctx ).writeAndFlush( ResponseMessageType.FILE );
        verify( ctx ).writeAndFlush( new FileHeader( smallFile.getName() ) );
        verify( ctx ).writeAndFlush( FileChunk.create( bytes, true ) );
        verifyNoMoreInteractions( ctx );
    }

    @Test
    public void sendLargeFile() throws IOException
    {
        // given
        int dataSize = MAX_SIZE + (MAX_SIZE / 2);
        byte[] bytes = new byte[dataSize];
        random.nextBytes( bytes );

        FileSender fileSender = new FileSender( fs, ctx );
        File smallFile = testDirectory.file( "smallFile" );
        try( StoreChannel storeChannel = fs.create( smallFile ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }

        // when
        fileSender.sendFile( smallFile );

        // then
        verify( ctx ).writeAndFlush( ResponseMessageType.FILE );
        verify( ctx ).writeAndFlush( new FileHeader( smallFile.getName() ) );
        verify( ctx ).writeAndFlush( FileChunk.create( copyOfRange( bytes, 0, MAX_SIZE ), false ) );
        verify( ctx ).writeAndFlush( FileChunk.create( copyOfRange( bytes, MAX_SIZE, bytes.length ), true ) );
        verifyNoMoreInteractions( ctx );
    }

    @Test
    public void sendLargeFileWithSizeMultipleOfTheChunkSize() throws IOException
    {
        // given
        byte[] bytes = new byte[MAX_SIZE * 3];
        random.nextBytes( bytes );

        FileSender fileSender = new FileSender( fs, ctx );
        File smallFile = testDirectory.file( "smallFile" );
        try( StoreChannel storeChannel = fs.create( smallFile ) )
        {
            storeChannel.write( ByteBuffer.wrap( bytes ) );
        }

        // when
        fileSender.sendFile( smallFile );

        // then
        verify( ctx ).writeAndFlush( ResponseMessageType.FILE );
        verify( ctx ).writeAndFlush( new FileHeader( smallFile.getName() ) );
        verify( ctx ).writeAndFlush( FileChunk.create( copyOfRange( bytes, 0, MAX_SIZE ), false ) );
        verify( ctx ).writeAndFlush( FileChunk.create( copyOfRange( bytes, MAX_SIZE, MAX_SIZE * 2 ), false ) );
        verify( ctx ).writeAndFlush( FileChunk.create( copyOfRange( bytes, MAX_SIZE * 2, bytes.length ), true ) );
        verifyNoMoreInteractions( ctx );
    }
}
