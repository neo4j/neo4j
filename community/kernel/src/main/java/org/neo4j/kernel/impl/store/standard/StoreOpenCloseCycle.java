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
package org.neo4j.kernel.impl.store.standard;

import static java.nio.ByteBuffer.wrap;
import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.io.fs.FileUtils.windowsSafeIOOperation;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.OverlappingFileLockException;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.FileLock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.NotCurrentStoreVersionException;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Manages the "opening" and "closing" of store files. In this context, a "closed" store is one that has a footer
 * appended at the end of the file, while an "open" store has had that footer removed. Using this footer, we can
 * determine if a store was cleanly closed (contains footer) or not (no footer), as well as ensure that the format
 * is one we can read.
 *
 * The footer contains two values written to the end of the store on shutdown.
 *
 * - A store "type descriptor", such as "NodeStore", which does not change across versions
 * - A store version, which must change when store format changes. Must always be 6 bytes long when serialized to UTF-8,
 *   because of reasons.
 *
 * The various states and how to read them looks like:
 *
 *                   Correct type | Wrong/missing Type
 * Correct version      CLEAN     |      UNCLEAN
 *   Wrong version  WRONG_VERSION |      UNCLEAN
 *
 */
public class StoreOpenCloseCycle
{
    enum StoreState
    {
        /** Store has been correctly shut down, and can be used directly. */
        CLEAN,

        /** Store is correctly shut down, but has a version different from the one expected. */
        WRONG_VERSION,

        /**
         * Store is incorrectly shut down, and requires recovery. The version is unknown,
         * and must be securely determined by some other mechanism.
         */
        UNCLEAN
    }

    /**
     * Wraps a {@link StoreOpenCloseCycle.StoreState} with a description of the reason
     * for it, to help with error messages.
     */
    public static class StateDescription
    {
        private final StoreState state;
        private final String expectedFooter;
        private final String foundTypeAndVersion;

        public StateDescription( StoreState state, String expectedFooter, String foundTypeAndVersion )
        {
            this.state = state;
            this.expectedFooter = expectedFooter;
            this.foundTypeAndVersion = foundTypeAndVersion;
        }

        public StoreState state()
        {
            return state;
        }

        public String expectedFooter()
        {
            return expectedFooter;
        }

        public String foundFooter()
        {
            return foundTypeAndVersion;
        }
    }

    private final StringLogger log;
    private final File dbFileName;
    private final StoreFormat<?, ?> format;
    private final FileSystemAbstraction fs;

    private FileLock fileLock;

    public StoreOpenCloseCycle( StringLogger log, File dbFileName, StoreFormat<?, ?> format, FileSystemAbstraction fs )
    {
        this.log = log;
        this.dbFileName = dbFileName;
        this.format = format;
        this.fs = fs;
    }

    /**
     * Opens the store, truncating off the footer if it exists.
     * Returns 'true' if the store has not been cleanly shut down and the id
     * generator should be rebuilt, 'false' otherwise.
     */
    public boolean openStore( StoreChannel channel ) throws IOException
    {
        lock( channel );
        StateDescription result = determineState( channel );
        StoreState state = result.state();
        switch ( state )
        {
            case CLEAN:
                // Cut off the footer, indicating the store is open
                channel.truncate( channel.size() - UTF8.encode( footer() ).length );
                return false;
            case UNCLEAN:
                // Store was not closed properly, indicating a crash or some other event causing the process
                // to exit without running shut down procedures. We need to rebuild our id generator at this point.
                return true;
            case WRONG_VERSION:
                throw new NotCurrentStoreVersionException( result.expectedFooter(), result.foundFooter(), "", false );
            default:
                throw new IllegalStateException( "Unknown store state: " + state );
        }
    }

    public void closeStore( final StoreChannel channel, final long highestIdInUse ) throws IOException
    {
        windowsSafeIOOperation( new FileUtils.FileOperation()
        {
            @Override
            public void perform() throws IOException
            {
                channel.position( highestIdInUse * format.recordSize( channel ) );
                ByteBuffer buffer = wrap( encode( footer() ) );
                channel.write( buffer );
                log.debug( "Closing " + dbFileName + ", truncating at " + channel.position() +
                        " vs file size " + channel.size() );
                channel.truncate( channel.position() );
                channel.force( false );
                if ( fileLock != null )
                {
                    fileLock.release();
                    fileLock = null;
                }
            }
        } );
    }

    private void lock( StoreChannel channel )
    {
        try
        {
            this.fileLock = fs.tryLock( dbFileName, channel );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to lock store[" + dbFileName + "]", e );
        }
        catch ( OverlappingFileLockException e )
        {
            throw new IllegalStateException( "Unable to lock store [" + dbFileName +
                    "], this is usually caused by another Neo4j kernel already running in " +
                    "this JVM for this particular store" );
        }
    }

    private StateDescription determineState( StoreChannel channel ) throws IOException
    {
        assert format.version().getBytes( Charsets.UTF_8 ).length == 6: "Version must, for historical reasons, be 6 bytes long.";

        String expectedTypeAndVersion = footer();

        long fileSize = channel.size();
        byte[] expected = expectedTypeAndVersion.getBytes( Charsets.UTF_8 );
        byte[] found = new byte[expected.length];

        ByteBuffer buffer = ByteBuffer.wrap( found );

        if ( fileSize < expected.length )
        {
            // File is too small to have a footer, must be unclean
            return new StateDescription(StoreState.UNCLEAN, expectedTypeAndVersion, "None" );
        }

        int recordSize = format.recordSize( channel );
        if( recordSize != 0 && (fileSize - expected.length) % recordSize != 0)
        {
            return new StateDescription(StoreState.UNCLEAN, expectedTypeAndVersion, "None" );
        }

        channel.position( fileSize - expected.length );
        channel.read( buffer );

        String foundTypeAndVersion = UTF8.decode( found );

        if ( !expectedTypeAndVersion.equals( foundTypeAndVersion ) )
        {
            if ( foundTypeAndVersion.startsWith( format.type() ) )
            {
                return new StateDescription( StoreState.WRONG_VERSION, expectedTypeAndVersion, foundTypeAndVersion );
            }
            else
            {
                return new StateDescription( StoreState.UNCLEAN, expectedTypeAndVersion, foundTypeAndVersion );
            }
        }

        return new StateDescription( StoreState.CLEAN, expectedTypeAndVersion, foundTypeAndVersion );
    }

    private String footer()
    {
        return format.type() + " " + format.version();
    }
}
