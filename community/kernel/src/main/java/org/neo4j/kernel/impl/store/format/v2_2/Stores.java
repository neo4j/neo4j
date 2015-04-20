/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.v2_2;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.standard.StandardStore;
import org.neo4j.kernel.impl.store.standard.StoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreIdGenerator;
import org.neo4j.kernel.impl.util.StringLogger;

public final class Stores
{
    public static class NodeStore_v2_2 extends StandardStore<NodeRecord, NodeStoreFormat_v2_2.NodeRecordCursor>
    {
        public NodeStore_v2_2(File dbFileName, StoreIdGenerator idGenerator, PageCache pageCache, FileSystemAbstraction fs, StringLogger log )
        {
            super( new NodeStoreFormat_v2_2(), dbFileName, idGenerator, pageCache, fs, log );
        }
    }
    public static class RelStore_v2_2 extends StandardStore<RelationshipRecord, RelationshipStoreFormat_v2_2.RelationshipRecordCursor>
    {
        public RelStore_v2_2( File dbFileName, StoreIdGenerator idGenerator, PageCache pageCache, FileSystemAbstraction fs, StringLogger log )
        {
            super( new RelationshipStoreFormat_v2_2(), dbFileName, idGenerator, pageCache, fs, log );
        }
    }
    public static class RelGroupStore_v2_2 extends StandardStore<RelationshipGroupRecord, RelationshipGroupStoreFormat_v2_2.RelationshipGroupRecordCursor>
    {
        public RelGroupStore_v2_2( StoreFormat<RelationshipGroupRecord, RelationshipGroupStoreFormat_v2_2.RelationshipGroupRecordCursor> format, File dbFileName, StoreIdGenerator idGenerator, PageCache pageCache, FileSystemAbstraction fs, StringLogger log )
        {
            super( format, dbFileName, idGenerator, pageCache, fs, log );
        }
    }
}
