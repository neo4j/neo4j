/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.pushtocloud;

import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.helpers.progress.ProgressListener;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ProgressTrackingOutputStreamTest
{
    @Test
    public void shouldTrackSingleByteWrites() throws IOException
    {
        // given
        OutputStream actual = mock( OutputStream.class );
        ProgressListener progressListener = mock( ProgressListener.class );
        ProgressTrackingOutputStream.Progress progress = new ProgressTrackingOutputStream.Progress( progressListener, 0 );
        try ( ProgressTrackingOutputStream out = new ProgressTrackingOutputStream( actual, progress ) )
        {
            // when
            out.write( 10 );
        }
        progress.done();

        // then
        verify( progressListener ).add( 1 );
        verify( progressListener ).done();
        verifyNoMoreInteractions( progressListener );
    }

    @Test
    public void shouldTrackByteArrayWrites() throws IOException
    {
        // given
        OutputStream actual = mock( OutputStream.class );
        ProgressListener progressListener = mock( ProgressListener.class );
        ProgressTrackingOutputStream.Progress progress = new ProgressTrackingOutputStream.Progress( progressListener, 0 );
        int length = 14;
        try ( ProgressTrackingOutputStream out = new ProgressTrackingOutputStream( actual, progress ) )
        {
            // when
            out.write( new byte[length] );
        }
        progress.done();

        // then
        verify( progressListener ).add( length );
        verify( progressListener ).done();
        verifyNoMoreInteractions( progressListener );
    }

    @Test
    public void shouldTrackOffsetByteArrayWrites() throws IOException
    {
        // given
        OutputStream actual = mock( OutputStream.class );
        ProgressListener progressListener = mock( ProgressListener.class );
        ProgressTrackingOutputStream.Progress progress = new ProgressTrackingOutputStream.Progress( progressListener, 0 );
        int length = 5;
        try ( ProgressTrackingOutputStream out = new ProgressTrackingOutputStream( actual, progress ) )
        {
            // when
            out.write( new byte[length * 2], 2, length );
        }
        progress.done();

        // then
        verify( progressListener ).add( length );
        verify( progressListener ).done();
        verifyNoMoreInteractions( progressListener );
    }

    @Test
    public void shouldTrackOffsetAfterRewind() throws IOException
    {
        // given
        OutputStream actual = mock( OutputStream.class );
        ProgressListener progressListener = mock( ProgressListener.class );
        ProgressTrackingOutputStream.Progress progress = new ProgressTrackingOutputStream.Progress( progressListener, 0 );
        try ( ProgressTrackingOutputStream out = new ProgressTrackingOutputStream( actual, progress ) )
        {
            out.write( new byte[20] );

            // when
            progress.rewindTo( 15 ); // i.e. the next 5 bytes we don't track
            out.write( new byte[3] ); // now there should be 2 untracked bytes left
            out.write( new byte[9] ); // this one should report 7
        }
        progress.done();

        // then
        InOrder inOrder = inOrder( progressListener );
        inOrder.verify( progressListener ).add( 20 );
        inOrder.verify( progressListener ).add( 7 );
        inOrder.verify( progressListener ).done();
        verifyNoMoreInteractions( progressListener );
    }

    @Test
    public void shouldNoteInitialPosition()
    {
        // given
        ProgressListener progressListener = mock( ProgressListener.class );

        // when
        new ProgressTrackingOutputStream.Progress( progressListener, 10 );

        // then
        verify( progressListener ).add( 10 );
        verifyNoMoreInteractions( progressListener );
    }
}
