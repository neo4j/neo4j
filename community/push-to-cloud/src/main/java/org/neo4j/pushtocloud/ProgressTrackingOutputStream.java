/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.helpers.progress.ProgressListener;

class ProgressTrackingOutputStream extends OutputStream
{
    private final OutputStream actual;
    private final Progress progress;

    ProgressTrackingOutputStream( OutputStream actual, Progress progress )
    {
        this.actual = actual;
        this.progress = progress;
    }

    @Override
    public void write( byte[] b, int off, int len ) throws IOException
    {
        actual.write( b, off, len );
        progress.add( len );
    }

    @Override
    public void flush() throws IOException
    {
        actual.flush();
    }

    @Override
    public void close() throws IOException
    {
        actual.close();
    }

    @Override
    public void write( int b ) throws IOException
    {
        actual.write( b );
        progress.add( 1 );
    }

    static class Progress
    {
        private final ProgressListener uploadProgress;
        // Why have this as a separate field here? Because we will track local progress while streaming the file,
        // i.e. how much we send. But if the upload gets aborted we may take a small step backwards after asking about resume position
        // and so to play nice with out progress listener (e.g. hard to remove printed dots from the terminal)
        // we won't report until we're caught up with it.
        private long highestReportedProgress;
        private long progress;
        private boolean done;

        /**
         * @param progressListener {@link ProgressListener} to report upload progress to.
         * @param position initial position to start the upload from. This is only useful if the upload was started and made it part-way
         * there before the command failed and the command has to be reissued at which point it can be resumed. This position is the position
         * where the upload will continue from. This is separate from temporary failure where the upload will be retried after some back-off.
         * That logic will instead make use of {@link #rewindTo(long)}.
         */
        Progress( ProgressListener progressListener, long position )
        {
            uploadProgress = progressListener;
            if ( position > 0 )
            {
                uploadProgress.add( position );
            }
        }

        void add( int increment )
        {
            progress += increment;
            if ( progress > highestReportedProgress )
            {
                uploadProgress.add( progress - highestReportedProgress );
                highestReportedProgress = progress;
            }
        }

        void rewindTo( long absoluteProgress )
        {
            // May be lower than what we're at, but that's fine
            progress = absoluteProgress;
            // highestReportedProgress will be kept as it is so that we know when we're caught up to it once more
        }

        void done()
        {
            done = true;
            uploadProgress.done();
        }

        boolean isDone()
        {
            return done;
        }
    }
}
