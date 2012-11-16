/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogTokens.CLEAN;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogTokens.LOG1;
import static org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLogTokens.LOG2;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

/**
 * Used to figure out what logical log file to open when the database
 * starts up.
 */
public class XaLogicalLogFiles {

    private static final String ACTIVE_FILE_SUFFIX = ".active";
    private static final String LOG_2_SUFFIX = ".2";
    private static final String LOG_1_SUFFIX = ".1";

    public static enum State {
        /**
         * Old < b8 xaframework with no log rotation
         */
        LEGACY_WITHOUT_LOG_ROTATION,
        
        /**
         * No .active file to tell us the state of 
         * affairs, and no legacy logical log file.
         * TODO: Describe this state better.
         */
        NO_ACTIVE_FILE,
        
        /**
         * Cleanly shut down log files.
         */
        CLEAN,
        
        /**
         * Log 1 is currently active.
         */
        LOG_1_ACTIVE,
        
        /**
         * Log 2 is currently active.
         */
        LOG_2_ACTIVE,
        
        /**
         * Log 1 is active, but log 2 is also 
         * present.
         */
        DUAL_LOGS_LOG_1_ACTIVE,
        
        /**
         * Log 2 is active, but log 1 is also 
         * present.
         */
        DUAL_LOGS_LOG_2_ACTIVE
    }

    private File fileName;
    private FileSystemAbstraction fileSystem;
    
    public XaLogicalLogFiles(File fileName, FileSystemAbstraction fileSystem)
    {
        this.fileName = fileName;
        this.fileSystem = fileSystem;
    }

    public State determineState() throws IOException
    {
        File activeFileName = new File( fileName.getPath() + ACTIVE_FILE_SUFFIX);
        if ( !fileSystem.fileExists( activeFileName ) )
        {
            if ( fileSystem.fileExists( fileName ) )
            {
                // old < b8 xaframework with no log rotation and we need to
                // do recovery on it
                return State.LEGACY_WITHOUT_LOG_ROTATION;
            }
            else
            {
                return State.NO_ACTIVE_FILE;
            }
        }
        else
        {
            FileChannel fc = null;
            byte bytes[] = new byte[256];
            ByteBuffer buf = ByteBuffer.wrap( bytes );
            int read = 0;
            try {
                fc = fileSystem.open( activeFileName, "rw" );
                read = fc.read( buf );
            } finally 
            {
                if(fc != null) fc.close();
            }
            
            if ( read != 4 )
            {
                throw new IllegalStateException( "Read " + read +
                    " bytes from " + activeFileName + " but expected 4" );
            }
            buf.flip();
            char c = buf.asCharBuffer().get();
            if ( c == CLEAN )
            {
                return State.CLEAN;
            }
            else if ( c == LOG1 )
            {
                if ( !fileSystem.fileExists( getLog1FileName() ) )
                {
                    throw new IllegalStateException(
                        "Active marked as 1 but no " + getLog1FileName() + " exist" );
                }
                if ( fileSystem.fileExists( getLog2FileName() ) )
                {
                    return State.DUAL_LOGS_LOG_1_ACTIVE;
                }
                return State.LOG_1_ACTIVE;
            }
            else if ( c == LOG2 )
            {
                if ( !fileSystem.fileExists( getLog2FileName() ) )
                {
                    throw new IllegalStateException(
                        "Active marked as 2 but no " + getLog2FileName() + " exist" );
                }
                if ( fileSystem.fileExists( getLog1FileName() ) )
                {
                    return State.DUAL_LOGS_LOG_2_ACTIVE;
                }
                return State.LOG_2_ACTIVE;
            }
            else
            {
                throw new IllegalStateException( "Unknown active log: " + c );
            }
        }
    }

    public File getLog1FileName()
    {
        return new File( fileName.getPath() + LOG_1_SUFFIX);
    }

    public File getLog2FileName()
    {
        return new File( fileName.getPath() + LOG_2_SUFFIX);
    }

}
