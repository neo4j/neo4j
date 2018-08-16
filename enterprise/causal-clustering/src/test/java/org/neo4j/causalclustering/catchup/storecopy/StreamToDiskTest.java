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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        StreamToDiskProvider writerProvider = new StreamToDiskProvider( directory.databaseDir(), fs, monitors );

        // WHEN
        for ( StoreType type : StoreType.values() )
        {
            if ( type.isRecordStore() )
            {
                File[] files = directory.databaseLayout().file( type.getDatabaseFile() ).toArray( File[]::new );
                for ( File file : files )
                {
                    writeAndVerify( writerProvider, file );
                }
            }
        }
        writeAndVerify( writerProvider, directory.databaseLayout().labelScanStore() );
    }

    private void writeAndVerify( StreamToDiskProvider writerProvider, File file ) throws Exception
    {
        try ( StoreFileStream acquire = writerProvider.acquire( file.getName(), 16 ) )
        {
            acquire.write( DATA );
        }
        assertTrue( "Streamed file created.", fs.fileExists( file ) );
        assertEquals( DATA.length, fs.getFileSize( file ) );
    }
}
