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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsCompositeAllocator;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.SchemaRuleSerialization;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.util.Collections.singleton;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

public class SchemaStore extends AbstractDynamicStore implements Iterable<SchemaRule>
{
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "SchemaStore";
    public static final int BLOCK_SIZE = 56;

    public SchemaStore(
            File fileName,
            Config conf,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            RecordFormats recordFormats,
            OpenOption... openOptions )
    {
        super( fileName, conf, idType, idGeneratorFactory, pageCache, logProvider, TYPE_DESCRIPTOR, BLOCK_SIZE,
                recordFormats.dynamic(), recordFormats.storeVersion(), openOptions );
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, DynamicRecord record ) throws FAILURE
    {
        processor.processSchema( this, record );
    }

    public List<DynamicRecord> allocateFrom( SchemaRule rule )
    {
        List<DynamicRecord> records = new ArrayList<>();
        DynamicRecord record = getRecord( rule.getId(), nextRecord(), CHECK );
        DynamicRecordAllocator recordAllocator = new ReusableRecordsCompositeAllocator( singleton( record ), this );
        allocateRecordsFromBytes( records, rule.serialize(), recordAllocator );
        return records;
    }

    public Iterator<SchemaRule> loadAllSchemaRules()
    {
        return new SchemaStorage( this ).loadAllSchemaRules();
    }

    @Override
    public Iterator<SchemaRule> iterator()
    {
        return loadAllSchemaRules();
    }

    static SchemaRule readSchemaRule( long id, Collection<DynamicRecord> records, byte[] buffer )
            throws MalformedSchemaRuleException
    {
        ByteBuffer scratchBuffer = concatData( records, buffer );
        return SchemaRuleSerialization.deserialize( id, scratchBuffer );
    }
}
