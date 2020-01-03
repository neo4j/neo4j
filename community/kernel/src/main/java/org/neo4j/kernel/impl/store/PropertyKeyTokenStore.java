/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.io.File;
import java.nio.file.OpenOption;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.logging.LogProvider;

/**
 * Implementation of the property store.
 */
public class PropertyKeyTokenStore extends TokenStore<PropertyKeyTokenRecord>
{
    // Historical type descriptor, should be called PropertyKeyTokenStore
    public static final String TYPE_DESCRIPTOR = "PropertyIndexStore";

    public PropertyKeyTokenStore(
            File file,
            File idFile,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            DynamicStringStore nameStore,
            RecordFormats recordFormats,
            OpenOption... openOptions )
    {
        super( file, idFile, config, IdType.PROPERTY_KEY_TOKEN, idGeneratorFactory, pageCache, logProvider, nameStore, TYPE_DESCRIPTOR,
                recordFormats.propertyKeyToken(), recordFormats.storeVersion(), openOptions );
    }

    @Override
    public <FAILURE extends Exception> void accept( RecordStore.Processor<FAILURE> processor,
            PropertyKeyTokenRecord record ) throws FAILURE
    {
        processor.processPropertyKeyToken( this, record );
    }
}
