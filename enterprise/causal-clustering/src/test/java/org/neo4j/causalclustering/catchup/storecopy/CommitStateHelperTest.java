/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitStateHelperTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fsr = new DefaultFileSystemRule();

    private Config config;
    private CommitStateHelper commitStateHelper;

    @Before
    public void setUp()
    {
        File txLogLocation = new File( testDirectory.directory(), "txLogLocation" );
        config = Config.builder().withSetting( GraphDatabaseSettings.logical_logs_location, txLogLocation.getAbsolutePath() ).build();
        commitStateHelper = new CommitStateHelper( null, fsr, config );
    }

    @Test
    public void shouldThrowRuntimeExceptionIfDirectoryDoesNotExist()
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        assertFalse( txDir.exists() );

        try
        {
            commitStateHelper.hasTxLogs();
            fail();
        }
        catch ( RuntimeException e )
        {
            assertEquals( e.getMessage(), "Files was null. Incorrect directory or I/O error?" );
        }
    }

    @Test
    public void shouldNotHaveTxLogsIfDirectoryIsEmpty()
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        txDir.mkdir();

        assertFalse( commitStateHelper.hasTxLogs() );
    }

    @Test
    public void shouldNotHaveTxLogsIfDirectoryHasFilesWithIncorrectName() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        txDir.mkdir();

        fsr.create( new File( txDir, "foo.bar" ) );

        assertFalse( commitStateHelper.hasTxLogs() );
    }

    @Test
    public void shouldHaveTxLogsIfDirectoryHasTxFile() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        txDir.mkdir();
        fsr.create( new File( txDir, TransactionLogFiles.DEFAULT_NAME + ".0" ) );

        assertTrue( commitStateHelper.hasTxLogs() );
    }
}
