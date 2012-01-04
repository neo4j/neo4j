/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;

import java.io.InputStream;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.TargetDirectory;

public class TestTxEntries
{
    @Test
    @Ignore
    /*
     * Starts a JVM, executes a tx that fails on prepare and rollbacks,
     * triggering a bug where an extra start entry for that tx is written
     * in the xa log.
     */
    public void testStartEntryWrittenOnceOnRollback() throws Exception
    {
        String storeDir = TargetDirectory.forTest( TestTxEntries.class ).directory(
                "rollBack", true ).getAbsolutePath();
        Process process = Runtime.getRuntime().exec(
                new String[] { "java", "-cp",
                        System.getProperty( "java.class.path" ),
                        RollbackUnclean.class.getName(), storeDir } );
        InputStream stdout = process.getInputStream();
        InputStream stderr = process.getErrorStream();
        while ( stdout.read() >= 0 || stderr.read() > 0 )
        {
            // just consume everything
        }
        int exit = process.waitFor();
        assertEquals( 0, exit );
        // The bug tested by this case throws exception during recovery, below
        new EmbeddedGraphDatabase( storeDir ).shutdown();
    }
}
