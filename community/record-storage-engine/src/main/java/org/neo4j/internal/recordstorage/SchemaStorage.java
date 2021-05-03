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
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
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
import org.neo4j.token.TokenHolders;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SchemaStorage implements SchemaRuleAccess, org.neo4j.kernel.impl.storemigration.SchemaStorage
{
    private final SchemaStore schemaStore;
    private final TokenHolders tokenHolders;
    private final KernelVersionRepository versionSupplier;
    private final boolean tokenIndexFeatureOn;

    /**
     * @param versionSupplier Used to know whether or not to inject a rule for NLI (that was formerly labelscanstore).
     *                        Use metadatastore as versionSupplier if you are not absolutely sure that the injected
     *                        rule is never needed.
     */
    public SchemaStorage( SchemaStore schemaStore, TokenHolders tokenHolders, KernelVersionRepository versionSupplier, boolean tokenIndexFeatureOn )
    {
        this.schemaStore = schemaStore;
        this.tokenHolders = tokenHolders;
        this.versionSupplier = versionSupplier;
        this.tokenIndexFeatureOn = tokenIndexFeatureOn;
    }

    @Override
    public long newRuleId( CursorContext cursorContext )
    {
        return schemaStore.nextId( cursorContext );
    }

    @Override
    public Iterable<SchemaRule> getAll( CursorContext cursorContext )
    {
        return streamAllSchemaRules( false, cursorContext )::iterator;
    }

    @Override
    public SchemaRule loadSingleSchemaRule( long ruleId, CursorContext cursorContext ) throws MalformedSchemaRuleException
    {
        SchemaRecord record = schemaStore.newRecord();
        schemaStore.getRecord( ruleId, record, RecordLoad.NORMAL, cursorContext );
        return readSchemaRule( record, cursorContext );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll( CursorContext cursorContext )
    {
        return indexRules( streamAllSchemaRules( false, cursorContext ) ).iterator();
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAllIgnoreMalformed( CursorContext cursorContext )
    {
        return indexRules( streamAllSchemaRules( true, cursorContext ) ).iterator();
    }

    @Override
    public IndexDescriptor[] indexGetForSchema( SchemaDescriptorSupplier supplier, CursorContext cursorContext )
    {
        SchemaDescriptor schema = supplier.schema();
        return indexRules( streamAllSchemaRules( false, cursorContext ) )
                .filter( rule -> rule.schema().equals( schema ) )
                .toArray( IndexDescriptor[]::new );
    }

    @Override
    public IndexDescriptor indexGetForName( String indexName, CursorContext cursorContext )
    {
        return indexRules( streamAllSchemaRules( false, cursorContext ) )
                .filter( idx -> idx.getName().equals( indexName ) )
                .findAny().orElse( null );
    }

    @Override
    public ConstraintDescriptor constraintsGetSingle( ConstraintDescriptor descriptor, CursorContext cursorContext )
            throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        ConstraintDescriptor[] rules = constraintRules( streamAllSchemaRules( false, cursorContext ) )
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
    public Iterator<ConstraintDescriptor> constraintsGetAllIgnoreMalformed( CursorContext cursorContext )
    {
        return constraintRules( streamAllSchemaRules( true, cursorContext ) ).iterator();
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

    private PropertyRecord newInitialisedPropertyRecord( PropertyStore propertyStore, SchemaRule rule, CursorContext cursorContext )
    {
        PropertyRecord record = propertyStore.newRecord();
        record.setId( propertyStore.nextId( cursorContext ) );
        record.setSchemaRuleId( rule.getId() );
        record.setCreated();
        return record;
    }

    private void linkAndWritePropertyRecord( PropertyStore propertyStore, PropertyRecord record, long prevPropId, long nextProp, CursorContext cursorContext )
    {
        record.setInUse( true );
        record.setPrevProp( prevPropId );
        record.setNextProp( nextProp );
        propertyStore.updateRecord( record, cursorContext );
        propertyStore.setHighestPossibleIdInUse( record.getId() );
    }

    @Override
    public void deleteSchemaRule( SchemaRule rule, CursorContext cursorContext )
    {
        SchemaRecord record = schemaStore.newRecord();
        schemaStore.getRecord( rule.getId(), record, RecordLoad.CHECK, cursorContext );
        if ( record.inUse() )
        {
            long nextProp = record.getNextProp();
            record.setInUse( false );
            schemaStore.updateRecord( record, cursorContext );
            PropertyStore propertyStore = schemaStore.propertyStore();
            PropertyRecord props = propertyStore.newRecord();
            while ( nextProp != Record.NO_NEXT_PROPERTY.longValue() && propertyStore.getRecord( nextProp, props, RecordLoad.NORMAL, cursorContext ).inUse() )
            {
                nextProp = props.getNextProp();
                props.setInUse( false );
                propertyStore.updateRecord( props, cursorContext );
            }
        }
    }

    @VisibleForTesting
    Stream<SchemaRule> streamAllSchemaRules( boolean ignoreMalformed, CursorContext cursorContext )
    {
        long startId = schemaStore.getNumberOfReservedLowIds();
        long endId = schemaStore.getHighId();
        Stream<IndexDescriptor> nli = Stream.empty();
        // TODO the final condition will be:
        // tokenIndexFeatureOn && versionSupplier.kernelVersion().isLessThan( KernelVersion.VERSION_IN_WHICH_TOKEN_INDEXES_ARE_INTRODUCED )
        // but until KernelVersion.LATEST >= KernelVersion.VERSION_IN_WHICH_TOKEN_INDEXES_ARE_INTRODUCED
        // this condition would cause the index being injected even for newly created databases
        if ( tokenIndexFeatureOn && !versionSupplier.kernelVersion().isLatest() )
        {
            nli = Stream.of( IndexDescriptor.INJECTED_NLI );
        }

        return Stream.concat( LongStream.range( startId, endId )
                .mapToObj( id -> schemaStore.getRecord( id, schemaStore.newRecord(), RecordLoad.LENIENT_ALWAYS, cursorContext ) )
                .filter( AbstractBaseRecord::inUse )
                .flatMap( record -> readSchemaRuleThrowingRuntimeException( record, ignoreMalformed, cursorContext ) ), nli );
    }

    private Stream<IndexDescriptor> indexRules( Stream<SchemaRule> stream )
    {
        return stream
                .filter( rule -> rule instanceof IndexDescriptor )
                .map( rule -> (IndexDescriptor) rule );
    }

    private Stream<ConstraintDescriptor> constraintRules( Stream<SchemaRule> stream )
    {
        return stream
                .filter( rule -> rule instanceof ConstraintDescriptor )
                .map( rule -> (ConstraintDescriptor) rule );
    }

    private Stream<SchemaRule> readSchemaRuleThrowingRuntimeException( SchemaRecord record, boolean ignoreMalformed, CursorContext cursorContext )
    {
        try
        {
            return Stream.of( readSchemaRule( record, cursorContext ) );
        }
        catch ( MalformedSchemaRuleException e )
        {
            // In case we've raced with a record deletion, ignore malformed records that no longer appear to be in use.
            if ( !ignoreMalformed && schemaStore.isInUse( record.getId(), cursorContext ) )
            {
                throw new RuntimeException( e );
            }
        }
        return Stream.empty();
    }

    private SchemaRule readSchemaRule( SchemaRecord record, CursorContext cursorContext ) throws MalformedSchemaRuleException
    {
        return SchemaStore.readSchemaRule( record, schemaStore.propertyStore(), tokenHolders, cursorContext );
    }
}
