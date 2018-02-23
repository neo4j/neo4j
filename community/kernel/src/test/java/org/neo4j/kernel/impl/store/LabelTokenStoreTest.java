/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.logging.LogProvider;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.configuration.Config.defaults;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.kernel.impl.store.record.Record.NOT_IN_USE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

public class LabelTokenStoreTest
{
    private final File file = mock( File.class );
    private final IdGeneratorFactory generatorFactory = mock( IdGeneratorFactory.class );
    private final PageCache cache = mock( PageCache.class );
    private final LogProvider logProvider = mock( LogProvider.class );
    private final DynamicStringStore dynamicStringStore = mock( DynamicStringStore.class );
    private final PageCursor pageCursor = mock( PageCursor.class );
    private final Config config = defaults();

    @Test
    public void forceGetRecordSkipInUsecheck() throws IOException
    {
        LabelTokenStore store = new UnusedLabelTokenStore();
        LabelTokenRecord record = store.getRecord( 7, store.newRecord(), FORCE );
        assertFalse( record.inUse(), "Record should not be in use" );
    }

    @Test
    public void getRecord()
    {
        assertThrows( InvalidRecordException.class, () -> {
            when( pageCursor.getByte() ).thenReturn( NOT_IN_USE.byteValue() );

            LabelTokenStore store = new UnusedLabelTokenStore();
            store.getRecord( 7, store.newRecord(), NORMAL );
        } );
    }

    class UnusedLabelTokenStore extends LabelTokenStore
    {
        UnusedLabelTokenStore() throws IOException
        {
            super( file, config, generatorFactory, cache, logProvider, dynamicStringStore,
                    defaultFormat() );
            storeFile = mock( PagedFile.class );

            when( storeFile.io( any( Long.class ), any( Integer.class ) ) ).thenReturn( pageCursor );
            when( storeFile.pageSize() ).thenReturn( 1 );
            when( pageCursor.next() ).thenReturn( true );
        }
    }
}
