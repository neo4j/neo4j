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

import org.eclipse.collections.api.set.ImmutableSet;

import java.nio.file.OpenOption;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.logging.InternalLogProvider;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;

/**
 * Implementation of the property store.
 */
public class PropertyKeyTokenStore extends TokenStore<PropertyKeyTokenRecord>
{
    // Historical type descriptor, should be called PropertyKeyTokenStore
    public static final String TYPE_DESCRIPTOR = "PropertyIndexStore";

    public PropertyKeyTokenStore(
            Path path,
            Path idFile,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            InternalLogProvider logProvider,
            DynamicStringStore nameStore,
            RecordFormats recordFormats,
            DatabaseReadOnlyChecker readOnlyChecker,
            String databaseName,
            ImmutableSet<OpenOption> openOptions )
    {
        super( path, idFile, config, SchemaIdType.PROPERTY_KEY_TOKEN, idGeneratorFactory, pageCache, logProvider, nameStore, TYPE_DESCRIPTOR,
                recordFormats.propertyKeyToken(), recordFormats.storeVersion(), readOnlyChecker, databaseName, PROPERTY_KEY_TOKEN_CURSOR,
                DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR, openOptions );
    }
}
