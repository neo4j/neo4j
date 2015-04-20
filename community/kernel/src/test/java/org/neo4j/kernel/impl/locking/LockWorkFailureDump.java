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
package org.neo4j.kernel.impl.locking;

import java.io.File;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SingleLoggingService;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.kernel.impl.util.StringLogger.logger;
import static org.neo4j.test.TargetDirectory.forTest;

public class LockWorkFailureDump
{
    private final Class<?> testClass;

    public LockWorkFailureDump( Class<?> testClass )
    {
        this.testClass = testClass;
    }
    
    public File dumpState( Locks lm, LockWorker... workers )
    {
        LifeSupport life = new LifeSupport();
        File file = forTest( testClass ).file( "failure-dump-" + currentTimeMillis() );
        Logging logging = life.add( new SingleLoggingService( logger( file ) ) );
        life.start();
        try
        {
            //  * locks held by the lock manager
            lm.accept( new DumpLocksVisitor( logging.getMessagesLog( LockWorkFailureDump.class ) ) );
            //  * rag manager state;
            //  * workers state
            for ( LockWorker worker : workers )
            {
                // - what each is doing and have up to now
                logging.getMessagesLog( getClass() ).logLongMessage( "Worker " + worker, worker );
            }
            return file;
        }
        finally
        {
            life.shutdown();
        }
    }
}
