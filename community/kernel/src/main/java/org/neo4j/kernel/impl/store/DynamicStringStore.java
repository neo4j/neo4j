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

import java.io.File;
import java.nio.file.OpenOption;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.logging.LogProvider;

/**
 * Dynamic store that stores strings.
 */
public class DynamicStringStore extends AbstractDynamicStore
{
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "StringPropertyStore";

    public DynamicStringStore(
            File fileName,
            Config configuration,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            int dataSizeFromConfiguration,
            RecordFormat<DynamicRecord> recordFormat,
            String storeVersion,
            OpenOption... openOptions )
    {
        super( fileName, configuration, idType, idGeneratorFactory, pageCache,
                logProvider, TYPE_DESCRIPTOR, dataSizeFromConfiguration, recordFormat, storeVersion, openOptions );
    }

    @Override
    public <FAILURE extends Exception> void accept( RecordStore.Processor<FAILURE> processor, DynamicRecord record )
            throws FAILURE
    {
        processor.processString( this, record, idType );
    }
}
