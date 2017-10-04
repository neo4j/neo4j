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

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.labelscan.LabelScanStoreTest;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.PageCacheRule;

import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.api.scan.FullStoreChangeStream.EMPTY;

public class NativeLabelScanStoreTest extends LabelScanStoreTest
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    @Override
    protected LabelScanStore createLabelScanStore( FileSystemAbstraction fileSystemAbstraction, File rootFolder,
            FullStoreChangeStream fullStoreChangeStream, boolean usePersistentStore, boolean readOnly,
            LabelScanStore.Monitor monitor )
    {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( monitor );
        PageCache pageCache = pageCacheRule.getPageCache( fileSystemAbstraction );
        return new NativeLabelScanStore( pageCache, rootFolder,
                fullStoreChangeStream, readOnly, monitors, RecoveryCleanupWorkCollector.IMMEDIATE );
    }

    @Override
    protected Matcher<Iterable<? super String>> hasBareMinimumFileList()
    {
        return Matchers.hasItem( Matchers.equalTo( NativeLabelScanStore.FILE_NAME ) );
    }

    @Override
    protected void corruptIndex( FileSystemAbstraction fileSystem, File rootFolder ) throws IOException
    {
        File lssFile = new File( rootFolder, NativeLabelScanStore.FILE_NAME );
        scrambleFile( lssFile );
    }

    @Test
    public void shouldStartPopulationAgainIfNotCompletedFirstTime() throws Exception
    {
        // given
        // label scan store init but no start
        LifeSupport life = new LifeSupport();
        TrackingMonitor monitor = new TrackingMonitor();
        life.add( createLabelScanStore( fileSystemRule.get(), dir, EMPTY, true, false, monitor ) );
        life.init();
        assertTrue( monitor.noIndexCalled );
        monitor.reset();
        life.shutdown();

        // when
        // starting label scan store again
        life = new LifeSupport();
        life.add( createLabelScanStore( fileSystemRule.get(), dir, EMPTY, true, false, monitor ) );
        life.init();

        // then
        // label scan store should recognize it still needs to be rebuilt
        assertTrue( monitor.corruptedIndex );
        life.start();
        assertTrue( monitor.rebuildingCalled );
        assertTrue( monitor.rebuiltCalled );
        life.shutdown();
    }
}
