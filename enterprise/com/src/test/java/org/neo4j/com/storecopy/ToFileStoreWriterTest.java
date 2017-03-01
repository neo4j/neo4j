/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.com.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.com.DataProducer;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import static org.neo4j.kernel.impl.storemigration.StoreFileType.STORE;

public class ToFileStoreWriterTest
{
    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( fs );
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( fs ).around( directory ).around( pageCacheRule );

    @Test
    public void shouldLetPageCacheHandleRecordStoresAndNativeLabelScanStoreFiles() throws Exception
    {
        // GIVEN
        List<FileMoveAction> actions = new ArrayList<>();
        PageCache pageCache = spy( pageCacheRule.getPageCache( fs ) );
        ToFileStoreWriter writer = new ToFileStoreWriter( directory.absolutePath(), fs,
                new StoreCopyClient.Monitor.Adapter(), pageCache, actions );
        ByteBuffer tempBuffer = ByteBuffer.allocate( 128 );

        // WHEN
        for ( StoreType type : StoreType.values() )
        {
            if ( type.isRecordStore() )
            {
                String fileName = type.getStoreFile().fileName( STORE );
                writeAndVerifyWrittenThroughPageCache( pageCache, writer, tempBuffer, fileName );
            }
        }
        writeAndVerifyWrittenThroughPageCache( pageCache, writer, tempBuffer, NativeLabelScanStore.FILE_NAME );
    }

    private void writeAndVerifyWrittenThroughPageCache( PageCache pageCache, ToFileStoreWriter writer,
            ByteBuffer tempBuffer, String fileName )
            throws IOException
    {
        writer.write( fileName, new DataProducer( 16 ), tempBuffer, true, 16 );
        verify( pageCache ).map( eq( directory.file( fileName ) ), anyInt(), anyVararg() );
    }
}
