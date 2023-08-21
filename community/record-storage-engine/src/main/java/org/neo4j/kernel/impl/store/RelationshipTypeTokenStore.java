/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_REL_TYPE_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.REL_TYPE_TOKEN_CURSOR;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.logging.InternalLogProvider;

/**
 * Implementation of the relationship type store. Uses a dynamic store to store
 * relationship type names.
 */
public class RelationshipTypeTokenStore extends TokenStore<RelationshipTypeTokenRecord> {
    public static final String TYPE_DESCRIPTOR = "RelationshipTypeStore";

    public RelationshipTypeTokenStore(
            FileSystemAbstraction fileSystem,
            Path path,
            Path idFile,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            InternalLogProvider logProvider,
            DynamicStringStore nameStore,
            RecordFormats recordFormats,
            boolean readOnly,
            String databaseName,
            ImmutableSet<OpenOption> openOptions) {
        super(
                fileSystem,
                path,
                idFile,
                config,
                SchemaIdType.RELATIONSHIP_TYPE_TOKEN,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                logProvider,
                nameStore,
                TYPE_DESCRIPTOR,
                recordFormats.relationshipTypeToken(),
                readOnly,
                databaseName,
                REL_TYPE_TOKEN_CURSOR,
                DYNAMIC_REL_TYPE_TOKEN_CURSOR,
                openOptions);
    }
}
