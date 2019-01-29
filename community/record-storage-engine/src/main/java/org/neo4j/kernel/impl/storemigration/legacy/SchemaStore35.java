package org.neo4j.kernel.impl.storemigration.legacy;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsCompositeAllocator;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.SchemaRuleSerialization;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.SchemaRule;

import static java.util.Collections.singleton;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;

/**
 * The SchemaStore implementation from 3.5.x, used for reading the old schema store during schema store migration.
 */
public class SchemaStore35 extends AbstractDynamicStore
{
    // store version, each store ends with this string (byte encoded)
    public static final String TYPE_DESCRIPTOR = "SchemaStore";
    public static final int BLOCK_SIZE = 56;

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
    public <FAILURE extends Exception> void accept( RecordStore.Processor<FAILURE> processor, DynamicRecord record ) throws FAILURE
    {
        processor.processSchema( this, record );
    }

    public List<DynamicRecord> allocateFrom( SchemaRule rule )
    {
        List<DynamicRecord> records = new ArrayList<>();
        DynamicRecord record = getRecord( rule.getId(), nextRecord(), CHECK );
        DynamicRecordAllocator recordAllocator = new ReusableRecordsCompositeAllocator( singleton( record ), this );
        allocateRecordsFromBytes( records, SchemaRuleSerialization.serialize( rule ), recordAllocator );
        return records;
    }

    public static SchemaRule readSchemaRule( long id, Collection<DynamicRecord> records, byte[] buffer )
            throws MalformedSchemaRuleException
    {
        ByteBuffer scratchBuffer = concatData( records, buffer );
        return SchemaRuleSerialization.deserialize( id, scratchBuffer );
    }
}
