/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.com.storecopy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.com.DataProducer;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
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
                new StoreCopyClientMonitor.Adapter(), pageCache, actions );
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

    @Test
    public void fullPathFileNamesUsedForMonitoringBackup() throws IOException
    {
        // given
        AtomicBoolean wasActivated = new AtomicBoolean( false );
        StoreCopyClientMonitor monitor = new StoreCopyClientMonitor.Adapter()
        {
            @Override
            public void startReceivingStoreFile( String file )
            {
                assertTrue( file.contains( "expectedFileName" ) );
                wasActivated.set( true );
            }
        };

        // and
        List<FileMoveAction> actions = new ArrayList<>();
        PageCache pageCache = spy( pageCacheRule.getPageCache( fs ) );
        ToFileStoreWriter writer = new ToFileStoreWriter( directory.absolutePath(), fs,
                monitor, pageCache, actions );
        ByteBuffer tempBuffer = ByteBuffer.allocate( 128 );

        // when
        writer.write( "expectedFileName", new DataProducer( 16 ), tempBuffer, true, 16 );

        // then
        assertTrue( wasActivated.get() );
    }

    private void writeAndVerifyWrittenThroughPageCache( PageCache pageCache, ToFileStoreWriter writer,
            ByteBuffer tempBuffer, String fileName )
            throws IOException
    {
        File expected = new File( directory.absolutePath(), fileName );
        writer.write( fileName, new DataProducer( 16 ), tempBuffer, true, 16 );
        verify( pageCache ).map( eq( expected ), anyInt(), any() );
    }
}
