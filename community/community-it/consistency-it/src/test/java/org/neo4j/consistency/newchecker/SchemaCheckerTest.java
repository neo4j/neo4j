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
package org.neo4j.consistency.newchecker;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Consumer;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyKeyTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipTypeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.SchemaConsistencyReport;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.NodeExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.RelExistenceConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.neo4j.internal.helpers.collection.Iterables.first;
import static org.neo4j.internal.helpers.collection.Iterables.last;
import static org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider.DESCRIPTOR;

class SchemaCheckerTest extends CheckerTestBase
{
    private int label1;
    private int label2;
    private int relationshipType1;
    private int relationshipType2;
    private int propertyKey1;
    private int propertyKey2;
    private final int UNUSED = 99;
    private final MutableIntObjectMap<MutableIntSet> mandatoryNodeProperties = IntObjectMaps.mutable.empty();
    private final MutableIntObjectMap<MutableIntSet> mandatoryRelationshipProperties = IntObjectMaps.mutable.empty();
    private final String NAME = "name1";
    private final String NAME2 = "name2";

    @Override
    void initialData( KernelTransaction tx ) throws KernelException
    {
        TokenWrite tokenWrite = tx.tokenWrite();
        label1 = tokenWrite.labelGetOrCreateForName( "A" );
        label2 = tokenWrite.labelGetOrCreateForName( "B" );
        relationshipType1 = tokenWrite.relationshipTypeGetOrCreateForName( "A" );
        relationshipType2 = tokenWrite.relationshipTypeGetOrCreateForName( "B" );
        propertyKey1 = tokenWrite.propertyKeyGetOrCreateForName( "A" );
        propertyKey2 = tokenWrite.propertyKeyGetOrCreateForName( "B" );
    }

    @Test
    void shouldReportDuplicateRuleContent() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index1 = IndexPrototype.forSchema( SchemaDescriptor.forLabel( label1, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            IndexDescriptor index2 = IndexPrototype.forSchema( SchemaDescriptor.forLabel( label1, propertyKey1 ) )
                    .withName( NAME2 )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            schemaStorage.writeSchemaRule( index1 );
            schemaStorage.writeSchemaRule( index2 );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.duplicateRuleContent( any() ) );
    }

    @Test
    void shouldReportSchemaRuleNotOnline() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( label1, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            schemaStorage.writeSchemaRule( index );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.schemaRuleNotOnline( any() ) );
    }

    @Test
    void shouldReportLabelNotInUse() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index = IndexPrototype.forSchema( SchemaDescriptor.forLabel( UNUSED, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            schemaStorage.writeSchemaRule( index );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.labelNotInUse( any() ) );
    }

    @Test
    void shouldReportRelationshipTypeNotInUse() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index = IndexPrototype.forSchema( SchemaDescriptor.forRelType( UNUSED, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            schemaStorage.writeSchemaRule( index );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.relationshipTypeNotInUse( any() ) );
    }

    @Test
    void shouldReportPropertyKeyNotInUse() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index = IndexPrototype.forSchema( SchemaDescriptor.forRelType( relationshipType1, UNUSED ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            schemaStorage.writeSchemaRule( index );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.propertyKeyNotInUse( any() ) );
    }

    @Test
    void shouldReportMissingObligationUniquenessConstraint() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index1 = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( label1, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() )
                    .withOwningConstraintId( UNUSED );
            schemaStorage.writeSchemaRule( index1 );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.missingObligation( any() ) );
    }

    @Test
    void shouldReportMissingObligationConstraintIndexRule() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            UniquenessConstraintDescriptor constraintDescriptor = ConstraintDescriptorFactory.uniqueForLabel( label1, propertyKey1 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME )
                    .withOwnedIndexId( UNUSED );
            schemaStorage.writeSchemaRule( constraintDescriptor );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.missingObligation( any() ) );
    }

    @Test
    void shouldReportMalformedSchemaRule() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index = IndexPrototype.forSchema( SchemaDescriptor.forLabel( label1, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            schemaStorage.writeSchemaRule( index );
            SchemaRecord schemaRecord = schemaStore.getRecord( index.getId(), schemaStore.newRecord(), RecordLoad.NORMAL );
            propertyStore.updateRecord( new PropertyRecord( schemaRecord.getNextProp() ) );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, SchemaConsistencyReport::malformedSchemaRule );
    }

    @Test
    void shouldReportConstraintIndexRuleNotReferencingBack() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( label1, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            UniquenessConstraintDescriptor uniquenessConstraint = ConstraintDescriptorFactory.uniqueForLabel( label1, propertyKey1 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME )
                    .withOwnedIndexId( index.getId() );
            index = index.withOwningConstraintId( UNUSED );
            schemaStorage.writeSchemaRule( index );
            schemaStorage.writeSchemaRule( uniquenessConstraint );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.constraintIndexRuleNotReferencingBack( any() ) );
    }

    @Test
    void shouldReportUniquenessConstraintNotReferencingBack() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( label1, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            UniquenessConstraintDescriptor uniquenessConstraint = ConstraintDescriptorFactory.uniqueForLabel( label1, propertyKey1 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME )
                    .withOwnedIndexId( UNUSED );
            index = index.withOwningConstraintId( uniquenessConstraint.getId() );
            schemaStorage.writeSchemaRule( index );
            schemaStorage.writeSchemaRule( uniquenessConstraint );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.uniquenessConstraintNotReferencingBack( any() ) );
    }

    @Test
    void shouldReportDuplicateObligationForIndex() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index1 = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( label1, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            IndexDescriptor index2 = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( label2, propertyKey2 ) )
                    .withName( NAME2 )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            UniquenessConstraintDescriptor uniquenessConstraint = ConstraintDescriptorFactory.uniqueForLabel( label1, propertyKey1 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME )
                    .withOwnedIndexId( index1.getId() );
            index1 = index1.withOwningConstraintId( uniquenessConstraint.getId() );
            index2 = index2.withOwningConstraintId( uniquenessConstraint.getId() );
            schemaStorage.writeSchemaRule( index1 );
            schemaStorage.writeSchemaRule( index2 );
            schemaStorage.writeSchemaRule( uniquenessConstraint );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.duplicateObligation( any() ) );
    }

    @Test
    void shouldReportDuplicateObligationForConstraint() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            IndexDescriptor index = IndexPrototype.uniqueForSchema( SchemaDescriptor.forLabel( label1, propertyKey1 ) )
                    .withName( NAME )
                    .withIndexProvider( DESCRIPTOR )
                    .materialise( schemaStore.nextId() );
            UniquenessConstraintDescriptor uniquenessConstraint1 = ConstraintDescriptorFactory.uniqueForLabel( label1, propertyKey1 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME )
                    .withOwnedIndexId( index.getId() );
            UniquenessConstraintDescriptor uniquenessConstraint2 = ConstraintDescriptorFactory.uniqueForLabel( label2, propertyKey2 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME2 )
                    .withOwnedIndexId( index.getId() );
            index = index.withOwningConstraintId( uniquenessConstraint1.getId() );
            schemaStorage.writeSchemaRule( index );
            schemaStorage.writeSchemaRule( uniquenessConstraint1 );
            schemaStorage.writeSchemaRule( uniquenessConstraint2 );
        }

        // when
        check();

        // then
        expect( SchemaConsistencyReport.class, report -> report.duplicateObligation( any() ) );
    }

    @Test
    void shouldPopulateMandatoryPropertiesMap() throws Exception
    {
        // given
        try ( AutoCloseable ignored = tx() )
        {
            NodeExistenceConstraintDescriptor constraint1 = ConstraintDescriptorFactory
                    .existsForLabel( label1, propertyKey1 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME );
            NodeExistenceConstraintDescriptor constraint2 = ConstraintDescriptorFactory
                    .existsForLabel( label2, propertyKey1, propertyKey2 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME2 );
            RelExistenceConstraintDescriptor constraint3 = ConstraintDescriptorFactory
                    .existsForRelType( relationshipType1, propertyKey2 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME );
            RelExistenceConstraintDescriptor constraint4 = ConstraintDescriptorFactory
                    .existsForRelType( relationshipType2, propertyKey1, propertyKey2 )
                    .withId( schemaStore.nextId() )
                    .withName( NAME2 );
            schemaStorage.writeSchemaRule( constraint1 );
            schemaStorage.writeSchemaRule( constraint2 );
            schemaStorage.writeSchemaRule( constraint3 );
            schemaStorage.writeSchemaRule( constraint4 );
        }

        // when
        check();

        // then
        assertEquals( IntSets.mutable.of( propertyKey1 ), mandatoryNodeProperties.remove( label1 ) );
        assertEquals( IntSets.mutable.of( propertyKey1, propertyKey2 ), mandatoryNodeProperties.remove( label2 ) );
        assertTrue( mandatoryNodeProperties.isEmpty() );
        assertEquals( IntSets.mutable.of( propertyKey2 ), mandatoryRelationshipProperties.remove( relationshipType1 ) );
        assertEquals( IntSets.mutable.of( propertyKey1, propertyKey2 ), mandatoryRelationshipProperties.remove( relationshipType2 ) );
        assertTrue( mandatoryRelationshipProperties.isEmpty() );
    }

    @Test
    void shouldReportLabelTokenDynamicRecordNotInUse() throws Exception
    {
        testDynamicLabelTokenChainInconsistency( record -> first( record.getNameRecords() ).setInUse( false ),
                LabelTokenConsistencyReport.class, report -> report.nameBlockNotInUse( any() ) );
    }

    @Test
    void shouldReportNextLabelTokenDynamicRecordNotInUse() throws Exception
    {
        testDynamicLabelTokenChainInconsistency( record -> last( record.getNameRecords() ).setInUse( false ),
                DynamicConsistencyReport.class, report -> report.nextNotInUse( any() ) );
    }

    @Test
    void shouldReportDynamicLabelStringEmpty() throws Exception
    {
        testDynamicLabelTokenChainInconsistency( record -> first( record.getNameRecords() ).setData( new byte[0] ),
                DynamicConsistencyReport.class, DynamicConsistencyReport::emptyBlock );
    }

    @Test
    void shouldReportDynamicLabelStringRecordNotFullReferencesNext() throws Exception
    {
        testDynamicLabelTokenChainInconsistency( record ->
                {
                    DynamicRecord first = first( record.getNameRecords() );
                    first( record.getNameRecords() ).setData( Arrays.copyOf( first.getData(), first.getLength() / 2 ) );
                },
                DynamicConsistencyReport.class, DynamicConsistencyReport::recordNotFullReferencesNext );
    }

    @Test
    void shouldReportPropertyKeyTokenDynamicRecordNotInUse() throws Exception
    {
        testDynamicPropertyKeyTokenChainInconsistency( record -> first( record.getNameRecords() ).setInUse( false ),
                PropertyKeyTokenConsistencyReport.class, report -> report.nameBlockNotInUse( any() ) );
    }

    @Test
    void shouldReportNextPropertyKeyTokenDynamicRecordNotInUse() throws Exception
    {
        testDynamicPropertyKeyTokenChainInconsistency( record -> last( record.getNameRecords() ).setInUse( false ),
                DynamicConsistencyReport.class, report -> report.nextNotInUse( any() ) );
    }

    @Test
    void shouldReportDynamicPropertyKeyStringEmpty() throws Exception
    {
        testDynamicPropertyKeyTokenChainInconsistency( record -> first( record.getNameRecords() ).setData( new byte[0] ),
                DynamicConsistencyReport.class, DynamicConsistencyReport::emptyBlock );
    }

    @Test
    void shouldReportDynamicPropertyKeyStringRecordNotFullReferencesNext() throws Exception
    {
        testDynamicPropertyKeyTokenChainInconsistency( record ->
                {
                    DynamicRecord first = first( record.getNameRecords() );
                    first( record.getNameRecords() ).setData( Arrays.copyOf( first.getData(), first.getLength() / 2 ) );
                },
                DynamicConsistencyReport.class, DynamicConsistencyReport::recordNotFullReferencesNext );
    }

    @Test
    void shouldReportRelationshipTypeTokenDynamicRecordNotInUse() throws Exception
    {
        testDynamicRelationshipTypeTokenChainInconsistency( record -> first( record.getNameRecords() ).setInUse( false ),
                RelationshipTypeConsistencyReport.class, report -> report.nameBlockNotInUse( any() ) );
    }

    @Test
    void shouldReportNextRelationshipTypeTokenDynamicRecordNotInUse() throws Exception
    {
        testDynamicRelationshipTypeTokenChainInconsistency( record -> last( record.getNameRecords() ).setInUse( false ),
                DynamicConsistencyReport.class, report -> report.nextNotInUse( any() ) );
    }

    @Test
    void shouldReportDynamicRelationshipTypeStringEmpty() throws Exception
    {
        testDynamicRelationshipTypeTokenChainInconsistency( record -> first( record.getNameRecords() ).setData( new byte[0] ),
                DynamicConsistencyReport.class, DynamicConsistencyReport::emptyBlock );
    }

    @Test
    void shouldReportDynamicRelationshipTypeStringRecordNotFullReferencesNext() throws Exception
    {
        testDynamicRelationshipTypeTokenChainInconsistency(
                record ->
                {
                    DynamicRecord first = first( record.getNameRecords() );
                    first( record.getNameRecords() ).setData( Arrays.copyOf( first.getData(), first.getLength() / 2 ) );
                },
                DynamicConsistencyReport.class, DynamicConsistencyReport::recordNotFullReferencesNext );
    }

    private <R extends ConsistencyReport> void testDynamicRelationshipTypeTokenChainInconsistency( Consumer<RelationshipTypeTokenRecord> vandal,
            Class<R> reportClass, Consumer<R> report ) throws Exception
    {
        testDynamicTokenChainInconsistency( neoStores.getRelationshipTypeTokenStore(), TokenWrite::relationshipTypeGetOrCreateForName,
                vandal, reportClass, report );
    }

    private <R extends ConsistencyReport> void testDynamicLabelTokenChainInconsistency( Consumer<LabelTokenRecord> vandal,
            Class<R> reportClass, Consumer<R> report ) throws Exception
    {
        testDynamicTokenChainInconsistency( neoStores.getLabelTokenStore(), TokenWrite::labelGetOrCreateForName, vandal, reportClass, report );
    }

    private <R extends ConsistencyReport> void testDynamicPropertyKeyTokenChainInconsistency( Consumer<PropertyKeyTokenRecord> vandal,
            Class<R> reportClass, Consumer<R> report ) throws Exception
    {
        testDynamicTokenChainInconsistency( neoStores.getPropertyKeyTokenStore(), TokenWrite::propertyKeyGetOrCreateForName, vandal, reportClass, report );
    }

    private <TOKEN extends TokenRecord,R extends ConsistencyReport> void testDynamicTokenChainInconsistency( TokenStore<TOKEN> store,
            TokenCreator tokenCreator, Consumer<TOKEN> vandal, Class<R> expectedReportClass, Consumer<R> report )
            throws Exception
    {
        // given
        int tokenId;
        try ( KernelTransaction ktx = ktx() )
        {
            tokenId = tokenCreator.createToken( ktx.tokenWrite(), "Broken".repeat( 50 ) );
            ktx.commit();
        }

        try ( AutoCloseable ignored = tx() )
        {
            // (T)--->(D)---> (vandalized dynamic value chain)
            TOKEN record = store.getRecord( tokenId, store.newRecord(), RecordLoad.NORMAL );
            store.ensureHeavy( record );
            vandal.accept( record );
            DynamicStringStore nameStore = store.getNameStore();
            for ( DynamicRecord nameRecord : record.getNameRecords() )
            {
                nameStore.updateRecord( nameRecord );
            }
        }

        // when
        check();

        // then
        expect( expectedReportClass, report );
    }

    private void check() throws Exception
    {
        new SchemaChecker( context() ).check( mandatoryNodeProperties, mandatoryRelationshipProperties );
    }

    interface TokenCreator
    {
        int createToken( TokenWrite tokenWrite, String name ) throws KernelException;
    }
}
