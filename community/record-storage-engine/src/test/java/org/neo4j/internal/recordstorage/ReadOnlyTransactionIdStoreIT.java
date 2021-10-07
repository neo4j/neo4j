/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;

@EphemeralPageCacheExtension
class ReadOnlyTransactionIdStoreIT
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory directory;

    @Test
    void testPageCacheAccessOnTransactionIdStoreConstruction() throws IOException
    {
        // given
        var databaseLayout = Neo4jLayout.of( directory.homePath() ).databaseLayout( "db" );
        new StoreFactory( databaseLayout, Config.defaults(), new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, directory.getFileSystem(),
                NullLogProvider.getInstance(), PageCacheTracer.NULL, DatabaseReadOnlyChecker.writable() ).openAllNeoStores( true ).close();
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorContext = new CursorContext( pageCacheTracer.createPageCursorTracer( "testPageCacheAccessOnTransactionIdStoreConstruction" ) );

        // when
        new ReadOnlyTransactionIdStore( directory.getFileSystem(), pageCache, databaseLayout, cursorContext );

        // then
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat( cursorTracer.pins() ).isEqualTo( 4 );
        assertThat( cursorTracer.unpins() ).isEqualTo( 4 );
        assertThat( cursorTracer.hits() ).isEqualTo( 0 );
    }
}
