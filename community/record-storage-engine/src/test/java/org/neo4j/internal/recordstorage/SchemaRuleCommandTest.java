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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Predicate;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorPredicates;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.NodeLabelUpdateListener;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.util.concurrent.WorkSync;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

class SchemaRuleCommandTest
{
    private final int labelId = 2;
    private final int propertyKey = 8;
    private final long id = 0;
    private final long txId = 1337L;
    private final NeoStores neoStores = mock( NeoStores.class );
    private final MetaDataStore metaDataStore = mock( MetaDataStore.class );
    private final SchemaStore schemaStore = mock( SchemaStore.class );
    private final IndexUpdateListener indexes = mock( IndexUpdateListener.class );
    private final IndexUpdateListener indexUpdateListener = mock( IndexUpdateListener.class );
    private final SchemaCache schemaCache = mock( SchemaCache.class );
    private final StorageEngine storageEngine = mock( StorageEngine.class );
    private final NodeLabelUpdateListener labelUpdateListener = mock( NodeLabelUpdateListener.class );
    private NeoStoreBatchTransactionApplier storeApplier;
    private final WorkSync<NodeLabelUpdateListener,LabelUpdateWork> labelScanStoreSynchronizer = new WorkSync<>( labelUpdateListener );
    private final WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync = new WorkSync<>( indexUpdateListener );
    private final PropertyStore propertyStore = mock( PropertyStore.class );
    private final IndexBatchTransactionApplier indexApplier = new IndexBatchTransactionApplier( indexUpdateListener, labelScanStoreSynchronizer,
            indexUpdatesSync, mock( NodeStore.class ), propertyStore, storageEngine, schemaCache, new IndexActivator( indexes ) );
    private final BaseCommandReader reader = new PhysicalLogCommandReaderV4_0();
    private final IndexDescriptor rule = IndexPrototype.forSchema( SchemaDescriptor.forLabel( labelId, propertyKey ) ).withName( "index" ).materialise( id );

    @BeforeEach
    void setup()
    {
        Map<IdType,WorkSync<IdGenerator,IdGeneratorUpdateWork>> idGeneratorWorkSyncs = new EnumMap<>( IdType.class );
        for ( IdType idType : IdType.values() )
        {
            idGeneratorWorkSyncs.put( idType, new WorkSync<>( mock( IdGenerator.class ) ) );
        }
        storeApplier = new NeoStoreBatchTransactionApplier( INTERNAL, neoStores, mock( CacheAccessBackDoor.class ), LockService.NO_LOCK_SERVICE,
                idGeneratorWorkSyncs );
    }

    @Test
    void shouldWriteCreatedSchemaRuleToStore() throws Exception
    {
        // GIVEN
        SchemaRecord before = new SchemaRecord( id ).initialize( false, NO_NEXT_PROPERTY.longValue() );
        SchemaRecord after = new SchemaRecord( id ).initialize( true, 42 );

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        visitSchemaRuleCommand( storeApplier, new SchemaRuleCommand( before, after, rule ) );

        // THEN
        verify( schemaStore ).updateRecord( eq( after ), any() );
    }

    @Test
    void shouldCreateIndexForCreatedSchemaRule() throws Exception
    {
        // GIVEN
        SchemaRecord before = new SchemaRecord( id ).initialize( false, NO_NEXT_PROPERTY.longValue() );
        SchemaRecord after = new SchemaRecord( id ).initialize( true, 42 );
        after.setCreated();

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        visitSchemaRuleCommand( indexApplier, new SchemaRuleCommand( before, after, rule ) );

        // THEN
        verify( indexUpdateListener ).createIndexes( rule );
    }

    @Test
    void shouldSetLatestConstraintRule() throws Exception
    {
        // Given
        SchemaRecord before = new SchemaRecord( id ).initialize( true, 42 );
        before.setCreated();
        SchemaRecord after = new SchemaRecord( id ).initialize( true, 42 );
        after.setConstraint( true );

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );
        when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );

        ConstraintDescriptor schemaRule = ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKey ).withId( id ).withOwnedIndexId( 0 );

        // WHEN
        visitSchemaRuleCommand( storeApplier, new SchemaRuleCommand( before, after, schemaRule ) );

        // THEN
        verify( schemaStore ).updateRecord( eq( after ), any() );
        verify( metaDataStore ).setLatestConstraintIntroducingTx( txId );
    }

    @Test
    void shouldDropSchemaRuleFromStore() throws Exception
    {
        // GIVEN
        SchemaRecord before = new SchemaRecord( id ).initialize( true, 42 );
        before.setCreated();
        SchemaRecord after = new SchemaRecord( id ).initialize( false, NO_NEXT_PROPERTY.longValue() );

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        visitSchemaRuleCommand( storeApplier, new SchemaRuleCommand( before, after, rule ) );

        // THEN
        verify( schemaStore ).updateRecord( eq( after ), any() );
    }

    @Test
    void shouldDropSchemaRuleFromIndex() throws Exception
    {
        // GIVEN
        SchemaRecord before = new SchemaRecord( id ).initialize( true, 42 );
        before.setCreated();
        SchemaRecord after = new SchemaRecord( id ).initialize( false, NO_NEXT_PROPERTY.longValue() );

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        visitSchemaRuleCommand( indexApplier, new SchemaRuleCommand( before, after, rule ) );

        // THEN
        verify( indexUpdateListener ).dropIndex( rule );
    }

    @Test
    void shouldWriteSchemaRuleToLog() throws Exception
    {
        // GIVEN
        SchemaRecord before = new SchemaRecord( id ).initialize( false, NO_NEXT_PROPERTY.longValue() );
        SchemaRecord after = new SchemaRecord( id ).initialize( true, 42 );
        after.setCreated();

        SchemaRuleCommand command = new SchemaRuleCommand( before, after, rule );
        InMemoryClosableChannel buffer = new InMemoryClosableChannel();

        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        command.serialize( buffer );
        Command readCommand = reader.read( buffer );

        // THEN
        assertThat( readCommand ).isInstanceOf( SchemaRuleCommand.class );

        assertSchemaRule( (SchemaRuleCommand)readCommand );
    }

    @Test
    void shouldRecreateSchemaRuleWhenDeleteCommandReadFromDisk() throws Exception
    {
        // GIVEN
        SchemaRecord before = new SchemaRecord( id ).initialize( true, 42 );
        before.setCreated();
        SchemaRecord after = new SchemaRecord( id ).initialize( false, NO_NEXT_PROPERTY.longValue() );

        SchemaRuleCommand command = new SchemaRuleCommand( before, after, rule );
        InMemoryClosableChannel buffer = new InMemoryClosableChannel();
        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        command.serialize( buffer );
        Command readCommand = reader.read( buffer );

        // THEN
        assertThat( readCommand ).isInstanceOf( SchemaRuleCommand.class );

        assertSchemaRule( (SchemaRuleCommand) readCommand );
    }

    @SuppressWarnings( "OptionalGetWithoutIsPresent" )
    @RepeatedTest( 1000 )
    void writeAndReadOfArbitrarySchemaRules() throws Exception
    {
        RandomSchema randomSchema = new RandomSchema();
        SchemaRule rule = randomSchema.schemaRules().filter( indexBackedConstraintsWithoutIndexes() ).findFirst().get();
        long ruleId = rule.getId();

        SchemaRecord before = new SchemaRecord( ruleId ).initialize( false, NO_NEXT_PROPERTY.longValue() );
        SchemaRecord after = new SchemaRecord( ruleId ).initialize( true, 42 );
        after.setCreated();

        SchemaRuleCommand command = new SchemaRuleCommand( before, after, rule );
        InMemoryClosableChannel buffer = new InMemoryClosableChannel( (int) ByteUnit.kibiBytes( 5 ) );
        when( neoStores.getSchemaStore() ).thenReturn( schemaStore );

        // WHEN
        command.serialize( buffer );
        SchemaRuleCommand readCommand = (SchemaRuleCommand) reader.read( buffer );

        // THEN
        assertEquals( ruleId, readCommand.getKey() );
        assertThat( readCommand.getSchemaRule() ).isEqualTo( rule );
    }

    /**
     * When we get to committing a schema rule command that writes a constraint rule, it is illegal for an index-backed constraint rule to not have a reference
     * to an index that it owns. However, the {@link RandomSchema} might generate such {@link ConstraintDescriptor ConstraintDescriptors},
     * so we have to filter them out.
     */
    private Predicate<? super SchemaRule> indexBackedConstraintsWithoutIndexes()
    {
        return r ->
        {
            if ( r instanceof ConstraintDescriptor )
            {
                ConstraintDescriptor constraint = (ConstraintDescriptor) r;
                return constraint.isIndexBackedConstraint() && constraint.asIndexBackedConstraint().hasOwnedIndexId();
            }
            return true;
        };
    }

    private void assertSchemaRule( SchemaRuleCommand readSchemaCommand )
    {
        assertEquals( id, readSchemaCommand.getKey() );
        assertTrue( SchemaDescriptorPredicates.hasLabel( readSchemaCommand.getSchemaRule(), labelId ) );
        assertTrue( SchemaDescriptorPredicates.hasProperty( readSchemaCommand.getSchemaRule(), propertyKey ) );
    }

    private void visitSchemaRuleCommand( BatchTransactionApplier applier, SchemaRuleCommand command ) throws Exception
    {
        CommandHandlerContract.apply( applier, new GroupOfCommands( txId, command ) );
    }
}
