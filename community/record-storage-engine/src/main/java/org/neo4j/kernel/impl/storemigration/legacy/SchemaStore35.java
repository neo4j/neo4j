/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.storemigration.legacy;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsCompositeAllocator;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

import static java.util.Collections.singleton;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * The SchemaStore implementation from 3.5.x, used for reading the old schema store during schema store migration.
 */
public class SchemaStore35 extends AbstractDynamicStore
{
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "SchemaStore";
    private static final int BLOCK_SIZE = 56;

    public SchemaStore35(
            File file,
            File idFile,
            Config conf,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            RecordFormats recordFormats,
            OpenOption... openOptions )
    {
        super( file, idFile, conf, idType, idGeneratorFactory, pageCache, logProvider, TYPE_DESCRIPTOR, BLOCK_SIZE,
                recordFormats.dynamic(), recordFormats.storeVersion(), openOptions );
    }

    @Override
    public <FAILURE extends Exception> void accept( RecordStore.Processor<FAILURE> processor, DynamicRecord record )
    {
        throw new UnsupportedOperationException( "Not needed for store migration." );
    }

    @VisibleForTesting
    @Override
    public void initialise( boolean createIfNotExists )
    {
        super.initialise( createIfNotExists );
    }

    public List<DynamicRecord> allocateFrom( SchemaRule rule )
    {
        List<DynamicRecord> records = new ArrayList<>();
        DynamicRecord record = getRecord( rule.getId(), newRecord(), CHECK );
        DynamicRecordAllocator recordAllocator = new ReusableRecordsCompositeAllocator( singleton( record ), this );
        allocateRecordsFromBytes( records, SchemaRuleSerialization35.serialize( rule ), recordAllocator );
        return records;
    }

    static SchemaRule readSchemaRule( long id, Collection<DynamicRecord> records, byte[] buffer ) throws MalformedSchemaRuleException
    {
        ByteBuffer scratchBuffer = concatData( records, buffer );
        return SchemaRuleSerialization35.deserialize( id, scratchBuffer );
    }
}
