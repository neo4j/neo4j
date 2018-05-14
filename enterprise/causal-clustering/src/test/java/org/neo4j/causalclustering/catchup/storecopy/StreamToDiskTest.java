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
package org.neo4j.causalclustering.catchup.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.storemigration.StoreFileType.STORE;

public class StreamToDiskTest
{
    private static final byte[] DATA = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( fs ).around( directory ).around( pageCacheRule );

    @Test
    public void shouldLetPageCacheHandleRecordStoresAndNativeLabelScanStoreFiles() throws Exception
    {
        // GIVEN
        Monitors monitors = new Monitors();
        StreamToDiskProvider writerProvider = new StreamToDiskProvider( directory.absolutePath(), fs, monitors );

        // WHEN
        for ( StoreType type : StoreType.values() )
        {
            if ( type.isRecordStore() )
            {
                String fileName = type.getStoreFile().fileName( STORE );
                writeAndVerify( writerProvider, fileName );
            }
        }
        writeAndVerify( writerProvider, NativeLabelScanStore.FILE_NAME );
    }

    private void writeAndVerify( StreamToDiskProvider writerProvider, String fileName ) throws Exception
    {
        try ( StoreFileStream acquire = writerProvider.acquire( fileName, 16 ) )
        {
            acquire.write( DATA );
        }
        File expectedFile = new File( directory.absolutePath(), fileName );
        assertTrue( "Streamed file created.", fs.fileExists( expectedFile ) );
        assertEquals( DATA.length, fs.getFileSize( expectedFile ) );
    }
}
