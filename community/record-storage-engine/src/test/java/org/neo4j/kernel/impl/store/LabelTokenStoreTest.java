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
package org.neo4j.kernel.impl.store;

import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.DatabaseReadOnlyChecker;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.cursor.StoreCursorsAdapter;

import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class LabelTokenStoreTest extends TokenStoreTestTemplate<LabelTokenRecord>
{
    @Override
    protected PageCursor storeCursor()
    {
        return storeCursors.labelTokenStoreCursor();
    }

    @Override
    protected StoreCursors createCursors( TokenStore<LabelTokenRecord> store, DynamicStringStore nameStore )
    {
        return new LabelTokenStoreCursors( store, nameStore );
    }

    @Override
    protected TokenStore<LabelTokenRecord> instantiateStore( Path file, Path idFile, IdGeneratorFactory generatorFactory, PageCache pageCache,
            LogProvider logProvider, DynamicStringStore nameStore, RecordFormats formats, Config config )
    {
        return new LabelTokenStore( file, idFile, config, generatorFactory, pageCache, logProvider, nameStore, formats, DatabaseReadOnlyChecker.writable(),
                DEFAULT_DATABASE_NAME, immutable.empty() );
    }

    private static class LabelTokenStoreCursors extends StoreCursorsAdapter
    {
        private final TokenStore<?> store;
        private final DynamicStringStore nameStore;
        private PageCursor storeCursor;
        private PageCursor dynamicCursor;

        LabelTokenStoreCursors( TokenStore<?> store, DynamicStringStore nameStore )
        {
            this.store = store;
            this.nameStore = nameStore;
        }

        @Override
        public PageCursor labelTokenStoreCursor()
        {
            if ( storeCursor == null )
            {
                storeCursor = store.openPageCursorForReading( 0, CursorContext.NULL );
            }
            return storeCursor;
        }

        @Override
        public PageCursor dynamicLabelTokeStoreCursor()
        {
            if ( dynamicCursor == null )
            {
                dynamicCursor = nameStore.openPageCursorForReading( 0, CursorContext.NULL );
            }
            return dynamicCursor;
        }

        @Override
        public void close()
        {
            if ( dynamicCursor != null )
            {
                dynamicCursor.close();
                dynamicCursor = null;
            }
            if ( storeCursor != null )
            {
                storeCursor.close();
                storeCursor = null;
            }
        }
    }
}
