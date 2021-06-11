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
package org.neo4j.internal.recordstorage;

import org.eclipse.collections.api.block.procedure.primitive.IntObjectProcedure;
import org.eclipse.collections.api.map.primitive.IntObjectMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexRef;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.storageengine.api.cursor.CursorTypes.PROPERTY_CURSOR;
import static org.neo4j.storageengine.api.cursor.CursorTypes.SCHEMA_CURSOR;

public class SchemaStorage implements SchemaRuleAccess
{
    private final SchemaStore schemaStore;
    private final TokenHolders tokenHolders;
    private final KernelVersionRepository versionSupplier;

    /**
     * @param versionSupplier Used to know whether or not to inject a rule for NLI (that was formerly labelscanstore).
     *                        Use metadatastore as versionSupplier if you are not absolutely sure that the injected
     *                        rule is never needed.
     */
    public SchemaStorage( SchemaStore schemaStore, TokenHolders tokenHolders, KernelVersionRepository versionSupplier )
    {
        this.schemaStore = schemaStore;
        this.tokenHolders = tokenHolders;
        this.versionSupplier = versionSupplier;
    }

    @Override
    public long newRuleId( CursorContext cursorContext )
    {
        return schemaStore.nextId( cursorContext );
    }

    @Override
    public Iterable<SchemaRule> getAll( StoreCursors storeCursors )
    {
        return streamAllSchemaRules( false, storeCursors )::iterator;
    }

    @Override
    public SchemaRule loadSingleSchemaRule( long ruleId, StoreCursors storeCursors ) throws MalformedSchemaRuleException
    {
        SchemaRecord record = loadSchemaRecord( ruleId, storeCursors );
        return readSchemaRule( record, storeCursors );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( StoreCursors storeCursors )
    {
        return indexRules( streamAllSchemaRules( false, storeCursors ) ).iterator();
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAllIgnoreMalformed( StoreCursors storeCursors )
    {
        return indexRules( streamAllSchemaRules( true, storeCursors ) ).iterator();
    }

    @Override
    public Iterator<IndexDescriptor> tokenIndexes( StoreCursors storeCursors )
    {
        return indexRules( streamAllSchemaRules( true, storeCursors ) ).filter( IndexRef::isTokenIndex ).iterator();
    }

    @Override
    public IndexDescriptor[] indexGetForSchema( SchemaDescriptorSupplier supplier, StoreCursors storeCursors )
    {
        SchemaDescriptor schema = supplier.schema();
        return indexRules( streamAllSchemaRules( false, storeCursors ) )
                .filter( rule -> rule.schema().equals( schema ) )
                .toArray( IndexDescriptor[]::new );
    }

    @Override
    public IndexDescriptor indexGetForName( String indexName, StoreCursors storeCursors )
    {
        return indexRules( streamAllSchemaRules( false, storeCursors ) )
                .filter( idx -> idx.getName().equals( indexName ) )
                .findAny().orElse( null );
    }

    @Override
    public ConstraintDescriptor constraintsGetSingle( ConstraintDescriptor descriptor, StoreCursors storeCursors )
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        ConstraintDescriptor[] rules = constraintRules( streamAllSchemaRules( false, storeCursors ) )
                .filter( descriptor::equals )
                .toArray( ConstraintDescriptor[]::new );
        if ( rules.length == 0 )
        {
            throw new SchemaRuleNotFoundException( descriptor, tokenHolders );
        }
        if ( rules.length > 1 )
        {
            throw new DuplicateSchemaRuleException( descriptor, tokenHolders );
        }
        return rules[0];
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAllIgnoreMalformed( StoreCursors storeCursors )
    {
        return constraintRules( streamAllSchemaRules( true, storeCursors ) ).iterator();
    }

    @Override
    public SchemaRecordChangeTranslator getSchemaRecordChangeTranslator()
    {
        return new PropertyBasedSchemaRecordChangeTranslator()
        {
            @Override
            protected IntObjectMap<Value> asMap( SchemaRule rule ) throws KernelException
            {
                return SchemaStore.convertSchemaRuleToMap( rule, tokenHolders );
            }

            @Override
            protected void setConstraintIndexOwnerProperty( long constraintId, IntObjectProcedure<Value> proc ) throws KernelException
            {
                int propertyId = SchemaStore.getOwningConstraintPropertyKeyId( tokenHolders );
                proc.value( propertyId, Values.longValue( constraintId ) );
            }
        };
    }

    @Override
    public void writeSchemaRule( SchemaRule rule, CursorContext cursorContext, MemoryTracker memoryTracker ) throws KernelException
    {
        IntObjectMap<Value> protoProperties = SchemaStore.convertSchemaRuleToMap( rule, tokenHolders );
        PropertyStore propertyStore = schemaStore.propertyStore();
        Collection<PropertyBlock> blocks = new ArrayList<>();
        protoProperties.forEachKeyValue( ( keyId, value ) ->
        {
            PropertyBlock block = new PropertyBlock();
            propertyStore.encodeValue( block, keyId, value, cursorContext, memoryTracker );
            blocks.add( block );
        } );

        assert !blocks.isEmpty() : "Property blocks should have been produced for schema rule: " + rule;

        long nextPropId = Record.NO_NEXT_PROPERTY.longValue();
        PropertyRecord currRecord = newInitialisedPropertyRecord( propertyStore, rule, cursorContext );

        for ( PropertyBlock block : blocks )
        {
            if ( !currRecord.hasSpaceFor( block ) )
            {
                PropertyRecord nextRecord = newInitialisedPropertyRecord( propertyStore, rule, cursorContext );
                linkAndWritePropertyRecord( propertyStore, currRecord, nextRecord.getId(), nextPropId, cursorContext );
                nextPropId = currRecord.getId();
                currRecord = nextRecord;
            }
            currRecord.addPropertyBlock( block );
        }

        linkAndWritePropertyRecord( propertyStore, currRecord, Record.NO_PREVIOUS_PROPERTY.longValue(), nextPropId, cursorContext );
        nextPropId = currRecord.getId();

        SchemaRecord schemaRecord = schemaStore.newRecord();
        schemaRecord.initialize( true, nextPropId );
        schemaRecord.setId( rule.getId() );
        schemaRecord.setCreated();
        schemaStore.updateRecord( schemaRecord, cursorContext );
        schemaStore.setHighestPossibleIdInUse( rule.getId() );
    }

    private static PropertyRecord newInitialisedPropertyRecord( PropertyStore propertyStore, SchemaRule rule, CursorContext cursorContext )
    {
        PropertyRecord record = propertyStore.newRecord();
        record.setId( propertyStore.nextId( cursorContext ) );
        record.setSchemaRuleId( rule.getId() );
        record.setCreated();
        return record;
    }

    private static void linkAndWritePropertyRecord( PropertyStore propertyStore, PropertyRecord record, long prevPropId, long nextProp,
            CursorContext cursorContext )
    {
        record.setInUse( true );
        record.setPrevProp( prevPropId );
        record.setNextProp( nextProp );
        propertyStore.updateRecord( record, cursorContext );
        propertyStore.setHighestPossibleIdInUse( record.getId() );
    }

    private SchemaRecord loadSchemaRecord( long ruleId, StoreCursors storeCursors )
    {
        return schemaStore.getRecordByCursor( ruleId, schemaStore.newRecord(), RecordLoad.NORMAL, storeCursors.pageCursor( SCHEMA_CURSOR ) );
    }

    @VisibleForTesting
    Stream<SchemaRule> streamAllSchemaRules( boolean ignoreMalformed, StoreCursors storeCursors )
    {
        long startId = schemaStore.getNumberOfReservedLowIds();
        long endId = schemaStore.getHighId();
        Stream<IndexDescriptor> nli = Stream.empty();
        KernelVersion currentVersion;
        try
        {
            currentVersion = versionSupplier.kernelVersion();
        }
        catch ( IllegalStateException ignored )
        {
            // If KernelVersion is missing we are an older store.
            currentVersion = KernelVersion.V4_2;
        }

        if ( currentVersion.isLessThan( KernelVersion.VERSION_IN_WHICH_TOKEN_INDEXES_ARE_INTRODUCED ) )
        {
            nli = Stream.of( IndexDescriptor.INJECTED_NLI );
        }

        return Stream.concat( LongStream.range( startId, endId ).mapToObj(
                id -> schemaStore.getRecordByCursor( id, schemaStore.newRecord(), RecordLoad.LENIENT_ALWAYS, storeCursors.pageCursor( SCHEMA_CURSOR ) ) )
                .filter( AbstractBaseRecord::inUse )
                .flatMap( record -> readSchemaRuleThrowingRuntimeException( record, ignoreMalformed, storeCursors ) ), nli );
    }

    private static Stream<IndexDescriptor> indexRules( Stream<SchemaRule> stream )
    {
        return stream
                .filter( rule -> rule instanceof IndexDescriptor )
                .map( rule -> (IndexDescriptor) rule );
    }

    private static Stream<ConstraintDescriptor> constraintRules( Stream<SchemaRule> stream )
    {
        return stream
                .filter( rule -> rule instanceof ConstraintDescriptor )
                .map( rule -> (ConstraintDescriptor) rule );
    }

    private Stream<SchemaRule> readSchemaRuleThrowingRuntimeException( SchemaRecord record, boolean ignoreMalformed, StoreCursors storeCursors )
    {
        try
        {
            return Stream.of( readSchemaRule( record, storeCursors ) );
        }
        catch ( MalformedSchemaRuleException e )
        {
            // In case we've raced with a record deletion, ignore malformed records that no longer appear to be in use.
            if ( !ignoreMalformed && schemaStore.isInUse( record.getId(), storeCursors.pageCursor( SCHEMA_CURSOR ) ) )
            {
                throw new RuntimeException( e );
            }
        }
        return Stream.empty();
    }

    private SchemaRule readSchemaRule( SchemaRecord record, StoreCursors storeCursors ) throws MalformedSchemaRuleException
    {
        return SchemaStore.readSchemaRule( record, schemaStore.propertyStore(), tokenHolders, storeCursors );
    }
}
