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

import java.nio.file.OpenOption;
import java.nio.file.Path;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.logging.InternalLogProvider;

public class RelationshipGroupStore extends CommonAbstractStore<RelationshipGroupRecord, IntStoreHeader> {
    public static final String TYPE_DESCRIPTOR = "RelationshipGroupStore";

    public RelationshipGroupStore(
            FileSystemAbstraction fileSystem,
            Path path,
            Path idFile,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            InternalLogProvider logProvider,
            RecordFormats recordFormats,
            boolean readOnly,
            String databaseName,
            ImmutableSet<OpenOption> openOptions) {
        super(
                fileSystem,
                path,
                idFile,
                config,
                RecordIdType.RELATIONSHIP_GROUP,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                logProvider,
                TYPE_DESCRIPTOR,
                recordFormats.relationshipGroup(),
                new IntStoreHeaderFormat(config.get(GraphDatabaseSettings.dense_node_threshold)),
                readOnly,
                databaseName,
                openOptions);
    }
}
