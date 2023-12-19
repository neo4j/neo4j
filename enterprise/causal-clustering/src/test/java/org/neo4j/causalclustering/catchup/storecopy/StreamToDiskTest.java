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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
        PageCache pageCache = spy( pageCacheRule.getPageCache( fs ) );
        Monitors monitors = new Monitors();
        StreamToDiskProvider writerProvider = new StreamToDiskProvider( directory.absolutePath(), fs, pageCache, monitors );

        // WHEN
        for ( StoreType type : StoreType.values() )
        {
            if ( type.isRecordStore() )
            {
                String fileName = type.getStoreFile().fileName( STORE );
                writeAndVerifyWrittenThroughPageCache( pageCache, writerProvider, fileName );
            }
        }
        writeAndVerifyWrittenThroughPageCache( pageCache, writerProvider, NativeLabelScanStore.FILE_NAME );
    }

    private void writeAndVerifyWrittenThroughPageCache( PageCache pageCache, StreamToDiskProvider writerProvider, String fileName ) throws Exception
    {
        try ( StoreFileStream acquire = writerProvider.acquire( fileName, 16 ) )
        {
            acquire.write( DATA );
        }
        verify( pageCache ).map( eq( directory.file( fileName ) ), anyInt(), any() );
    }
}
