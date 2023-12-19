/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommitStateHelperTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fsr = new DefaultFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    private Config config;
    private CommitStateHelper commitStateHelper;
    private File storeDir;

    @Before
    public void setUp()
    {
        File txLogLocation = new File( testDirectory.directory(), "txLogLocation" );
        config = Config.builder().withSetting( GraphDatabaseSettings.logical_logs_location, txLogLocation.getAbsolutePath() ).build();
        storeDir = testDirectory.graphDbDir();
        commitStateHelper = new CommitStateHelper( pageCacheRule.getPageCache( fsr ), fsr, config );
    }

    @Test
    public void shouldNotHaveTxLogsIfDirectoryDoesNotExist() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        assertFalse( txDir.exists() );

        assertFalse( commitStateHelper.hasTxLogs( storeDir ) );
    }

    @Test
    public void shouldNotHaveTxLogsIfDirectoryIsEmpty() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        txDir.mkdir();

        assertFalse( commitStateHelper.hasTxLogs( storeDir ) );
    }

    @Test
    public void shouldNotHaveTxLogsIfDirectoryHasFilesWithIncorrectName() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        txDir.mkdir();

        fsr.create( new File( txDir, "foo.bar" ) );

        assertFalse( commitStateHelper.hasTxLogs( storeDir ) );
    }

    @Test
    public void shouldHaveTxLogsIfDirectoryHasTxFile() throws IOException
    {
        File txDir = config.get( GraphDatabaseSettings.logical_logs_location );
        txDir.mkdir();
        fsr.create( new File( txDir, TransactionLogFiles.DEFAULT_NAME + ".0" ) );

        assertTrue( commitStateHelper.hasTxLogs( storeDir ) );
    }
}
