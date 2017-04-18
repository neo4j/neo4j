/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.labelscan;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.api.scan.FullStoreChangeStream.EMPTY;
import static org.neo4j.kernel.impl.api.scan.FullStoreChangeStream.asStream;

public class NativeLabelScanStoreRebuildTest
{
    private PageCacheRule pageCacheRule = new PageCacheRule();
    private FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule ).around( testDirectory );

    private static final FullStoreChangeStream THROWING_STREAM = writer ->
    {
        throw new IllegalArgumentException();
    };
    private File storeDir;

    @Before
    public void setup()
    {
        storeDir = testDirectory.graphDbDir();
    }

    @Test
    public void mustBeDirtyIfFailedDuringRebuild() throws Exception
    {
        // given
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
        NativeLabelScanStore nativeLabelScanStore = null;
        try
        {
            nativeLabelScanStore =
                    new NativeLabelScanStore( pageCache, storeDir, THROWING_STREAM, false, new Monitors() );

            nativeLabelScanStore.init();
            nativeLabelScanStore.start();
        }
        catch ( IllegalArgumentException e )
        {
            if ( nativeLabelScanStore != null )
            {
                nativeLabelScanStore.shutdown();
            }
        }

        // when
        RecordingMonitor monitor = new RecordingMonitor();
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( monitor );

        nativeLabelScanStore =
                new NativeLabelScanStore( pageCache, storeDir, EMPTY, false, monitors );
        nativeLabelScanStore.init();
        nativeLabelScanStore.start();

        // then
        assertTrue( monitor.notValid );
        assertTrue( monitor.rebuilding );
        assertTrue( monitor.rebuilt );
        nativeLabelScanStore.shutdown();
    }

    @Test
    public void shouldFailOnUnsortedLabelsFromFullStoreChangeStream() throws Exception
    {
        // given
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemRule.get() );
        List<NodeLabelUpdate> existingData = new ArrayList<>();
        existingData.add( NodeLabelUpdate.labelChanges( 1, new long[0], new long[]{2, 1} ) );
        FullStoreChangeStream changeStream = asStream( existingData );
        NativeLabelScanStore nativeLabelScanStore = null;
        try
        {
            nativeLabelScanStore =
                    new NativeLabelScanStore( pageCache, storeDir, changeStream, false, new Monitors() );
            nativeLabelScanStore.init();

            // when
            nativeLabelScanStore.start();
            fail( "Expected native label scan store to fail on " );
        }
        catch ( IllegalArgumentException e )
        {
            // then
            assertThat( e.getMessage(), Matchers.containsString( "unsorted label" ) );
            assertThat( e.getMessage(), Matchers.stringContainsInOrder( Iterables.asIterable( "2", "1" ) ) );
        }
        finally
        {
            if ( nativeLabelScanStore != null )
            {
                nativeLabelScanStore.shutdown();
            }
        }
    }

    private class RecordingMonitor implements LabelScanStore.Monitor
    {
        boolean notValid;
        boolean rebuilding;
        boolean rebuilt;

        @Override
        public void notValidIndex()
        {
            notValid = true;
        }

        @Override
        public void rebuilding()
        {
            rebuilding = true;
        }

        @Override
        public void rebuilt( long roughNodeCount )
        {
            rebuilt = true;
        }
    }
}
