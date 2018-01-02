/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.stresstests.transaction.log;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.Callable;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.transaction.log.stresstest.TransactionAppenderStressTest.Builder;
import org.neo4j.kernel.impl.transaction.log.stresstest.TransactionAppenderStressTest.TransactionIdChecker;

import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.lang.System.getenv;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Notice the class name: this is _not_ going to be run as part of the main build.
 */
public class TransactionAppenderStressTesting
{
    private static final String DEFAULT_DURATION_IN_MINUTES = "5";
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ), "working" ).getPath();
    private static final String DEFAULT_NUM_THREADS = "10";

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Throwable
    {
        int durationInMinutes = parseInt( fromEnv( "TX_APPENDER_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        File workingDirectory = ensureExistsAndEmpty( fromEnv( "TX_APPENDER_WORKING_DIRECTORY", DEFAULT_WORKING_DIR ) );
        int threads = parseInt( fromEnv( "TX_APPENDER_NUM_THREADS", DEFAULT_NUM_THREADS ) );

        Callable<Long> runner = new Builder()
                .with( Builder.untilTimeExpired( durationInMinutes, MINUTES ) )
                .withWorkingDirectory( workingDirectory )
                .withNumThreads( threads )
                .build();

        long appendedTxs = runner.call();

        assertEquals( new TransactionIdChecker( workingDirectory ).parseAllTxLogs(), appendedTxs );

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( workingDirectory );
    }

    private File ensureExistsAndEmpty( String directory )
    {
        File dir = new File( directory );
        if ( !dir.mkdirs() )
        {
            assertTrue( dir.exists() );
            assertTrue( dir.isDirectory() );
            assertEquals( 0, dir.list().length );
        }
        return dir;
    }

    private static String fromEnv( String environmentVariableName, String defaultValue )
    {
        String environmentVariableValue = getenv( environmentVariableName );
        return environmentVariableValue == null ? defaultValue : environmentVariableValue;
    }
}
