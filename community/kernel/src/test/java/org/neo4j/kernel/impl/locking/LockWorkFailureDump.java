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
package org.neo4j.kernel.impl.locking;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;

public class LockWorkFailureDump
{
    private final File file;

    public LockWorkFailureDump( File file )
    {
        this.file = file;
    }

    public File dumpState( Locks lm, LockWorker... workers ) throws IOException
    {
        FileOutputStream out = new FileOutputStream( file, false );
        FormattedLogProvider logProvider = FormattedLogProvider.withoutAutoFlush().toOutputStream( out );

        try
        {
            //  * locks held by the lock manager
            lm.accept( new DumpLocksVisitor( logProvider.getLog( LockWorkFailureDump.class ) ) );
            //  * rag manager state;
            //  * workers state
            Log log = logProvider.getLog( getClass() );
            for ( LockWorker worker : workers )
            {
                // - what each is doing and have up to now
                log.info( "Worker %s", worker );
                worker.dump( log.infoLogger() );
            }
            return file;
        }
        finally
        {
            out.flush();
            out.close();
        }
    }
}
