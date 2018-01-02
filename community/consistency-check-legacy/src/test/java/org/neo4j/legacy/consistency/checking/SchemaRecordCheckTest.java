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
package org.neo4j.legacy.consistency.checking;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.RecordAccessStub;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SchemaRecordCheckTest
        extends RecordCheckTestBase<DynamicRecord, ConsistencyReport.SchemaConsistencyReport, SchemaRecordCheck>
{
    public SchemaRecordCheckTest()
    {
        super( new SchemaRecordCheck( configureSchemaStore() ),
                ConsistencyReport.SchemaConsistencyReport.class );
    }

    public static SchemaStorage configureSchemaStore()
    {
        return mock( SchemaStorage.class );
    }

    @Test
    public void shouldReportMalformedSchemaRule() throws Exception
    {
        // given
        DynamicRecord badRecord = inUse( new DynamicRecord( 0 ) );
        badRecord.setType( RecordAccessStub.SCHEMA_RECORD_TYPE );

        when( checker().ruleAccess.loadSingleSchemaRule( 0 ) ).thenThrow( new MalformedSchemaRuleException( "Bad Record" ) );

        // when
        ConsistencyReport.SchemaConsistencyReport report = check( badRecord );

        // then
        verify( report ).malformedSchemaRule();
    }

    @Test
    public void shouldReportInvalidLabelReferences() throws Exception
    {
        // given
        int schemaRuleId = 0;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record = inUse( dynamicRecord( schemaRuleId ) );
        SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );
        IndexRule rule = IndexRule.indexRule( schemaRuleId, labelId, propertyKeyId, providerDescriptor );
        when( checker().ruleAccess.loadSingleSchemaRule( schemaRuleId ) ).thenReturn( rule );

        LabelTokenRecord labelTokenRecord = add ( notInUse( new LabelTokenRecord( labelId ) ) );
        add(inUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        ConsistencyReport.SchemaConsistencyReport report = check( record );

        // then
        verify( report ).labelNotInUse( labelTokenRecord );
    }

    @Test
    public void shouldReportInvalidPropertyReferenceFromIndexRule() throws Exception
    {
        // given
        int schemaRuleId = 0;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record = inUse( dynamicRecord( schemaRuleId ) );
        SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );
        IndexRule rule = IndexRule.indexRule( schemaRuleId, labelId, propertyKeyId, providerDescriptor );
        when( checker().ruleAccess.loadSingleSchemaRule( schemaRuleId ) ).thenReturn( rule );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        PropertyKeyTokenRecord propertyKeyToken = add( notInUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        ConsistencyReport.SchemaConsistencyReport report = check( record );

        // then
        verify( report ).propertyKeyNotInUse( propertyKeyToken );
    }

    @Test
    public void shouldReportInvalidPropertyReferenceFromUniquenessConstraintRule() throws Exception
    {
        // given
        int schemaRuleId = 0;
        int indexRuleId = 1;

        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record = inUse( dynamicRecord( schemaRuleId ) );

        UniquePropertyConstraintRule rule = UniquePropertyConstraintRule
                .uniquenessConstraintRule( schemaRuleId, labelId, propertyKeyId, indexRuleId );

        when( checker().ruleAccess.loadSingleSchemaRule( schemaRuleId ) ).thenReturn( rule );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        PropertyKeyTokenRecord propertyKeyToken = add( notInUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        ConsistencyReport.SchemaConsistencyReport report = check( record );

        // then
        verify( report ).propertyKeyNotInUse( propertyKeyToken );
    }

    @Test
    public void shouldReportUniquenessConstraintNotReferencingBack() throws Exception
    {
        // given
        int ruleId1 = 0;
        int ruleId2 = 1;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record1 = inUse( dynamicRecord( ruleId1 ) );
        DynamicRecord record2 = inUse( dynamicRecord( ruleId2 ) );

        SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );

        IndexRule rule1 = IndexRule.constraintIndexRule( ruleId1, labelId, propertyKeyId, providerDescriptor, (long) ruleId2 );
        UniquePropertyConstraintRule rule2 = UniquePropertyConstraintRule
                .uniquenessConstraintRule( ruleId2, labelId, propertyKeyId, ruleId2 );

        when( checker().ruleAccess.loadSingleSchemaRule( ruleId1 ) ).thenReturn( rule1 );
        when( checker().ruleAccess.loadSingleSchemaRule( ruleId2 ) ).thenReturn( rule2 );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        add( inUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        check( record1 );
        check( record2 );
        SchemaRecordCheck obligationChecker = checker().forObligationChecking();
        check( obligationChecker, record1 );
        ConsistencyReport.SchemaConsistencyReport report = check( obligationChecker, record2 );

        // then
        verify( report ).uniquenessConstraintNotReferencingBack( record1 );
    }

    @Test
    public void shouldNotReportConstraintIndexRuleWithoutBackReference() throws Exception
    {
        // given
        int ruleId = 1;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record = inUse( dynamicRecord( ruleId ) );

        SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );

        IndexRule rule = IndexRule.constraintIndexRule( ruleId, labelId, propertyKeyId, providerDescriptor, null );

        when( checker().ruleAccess.loadSingleSchemaRule( ruleId ) ).thenReturn( rule );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        add( inUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        check( record );
        SchemaRecordCheck obligationChecker = checker().forObligationChecking();
        ConsistencyReport.SchemaConsistencyReport report = check( obligationChecker, record );

        // then
        verifyZeroInteractions( report );
    }

    @Test
    public void shouldReportTwoUniquenessConstraintsReferencingSameIndex() throws Exception
    {
        // given
        int ruleId1 = 0;
        int ruleId2 = 1;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record1 = inUse( dynamicRecord( ruleId1 ) );
        DynamicRecord record2 = inUse( dynamicRecord( ruleId2 ) );

        UniquePropertyConstraintRule rule1 = UniquePropertyConstraintRule
                .uniquenessConstraintRule( ruleId1, labelId, propertyKeyId, ruleId2 );
        UniquePropertyConstraintRule rule2 = UniquePropertyConstraintRule
                .uniquenessConstraintRule( ruleId2, labelId, propertyKeyId, ruleId2 );

        when( checker().ruleAccess.loadSingleSchemaRule( ruleId1 ) ).thenReturn( rule1 );
        when( checker().ruleAccess.loadSingleSchemaRule( ruleId2 ) ).thenReturn( rule2 );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        add( inUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        check( record1 );
        ConsistencyReport.SchemaConsistencyReport report = check( record2 );

        // then
        verify( report ).duplicateObligation( record1 );
    }

    @Test
    public void shouldReportUnreferencedUniquenessConstraint() throws Exception
    {
        // given
        int ruleId = 0;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record = inUse( dynamicRecord( ruleId ) );

        UniquePropertyConstraintRule rule = UniquePropertyConstraintRule
                .uniquenessConstraintRule( ruleId, labelId, propertyKeyId, ruleId );

        when( checker().ruleAccess.loadSingleSchemaRule( ruleId ) ).thenReturn( rule );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        add( inUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        check( record );
        SchemaRecordCheck obligationChecker = checker().forObligationChecking();
        ConsistencyReport.SchemaConsistencyReport report = check( obligationChecker, record );

        // then
        verify( report ).missingObligation( SchemaRule.Kind.CONSTRAINT_INDEX_RULE );
    }

    @Test
    public void shouldReportConstraintIndexNotReferencingBack() throws Exception
    {
        // given
        int ruleId1 = 0;
        int ruleId2 = 1;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record1 = inUse( dynamicRecord( ruleId1 ) );
        DynamicRecord record2 = inUse( dynamicRecord( ruleId2) );

        SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );

        IndexRule rule1 = IndexRule.constraintIndexRule( ruleId1, labelId, propertyKeyId, providerDescriptor, (long)
                ruleId1 );
        UniquePropertyConstraintRule rule2 = UniquePropertyConstraintRule
                .uniquenessConstraintRule( ruleId2, labelId, propertyKeyId, ruleId1 );

        when( checker().ruleAccess.loadSingleSchemaRule( ruleId1 ) ).thenReturn( rule1 );
        when( checker().ruleAccess.loadSingleSchemaRule( ruleId2 ) ).thenReturn( rule2 );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        add( inUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        check( record1 );
        check( record2 );
        SchemaRecordCheck obligationChecker = checker().forObligationChecking();
        ConsistencyReport.SchemaConsistencyReport report = check( obligationChecker, record1 );
        check( obligationChecker, record2 );

        // then
        verify( report ).constraintIndexRuleNotReferencingBack( record2 );
    }

    @Test
    public void shouldReportTwoConstraintIndexesReferencingSameConstraint() throws Exception
    {
        // given
        int ruleId1 = 0;
        int ruleId2 = 1;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record1 = inUse( dynamicRecord( ruleId1 ) );
        DynamicRecord record2 = inUse( dynamicRecord( ruleId2 ) );

        SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );

        IndexRule rule1 = IndexRule.constraintIndexRule( ruleId1, labelId, propertyKeyId, providerDescriptor, (long) ruleId1 );
        IndexRule rule2 = IndexRule.constraintIndexRule( ruleId2, labelId, propertyKeyId, providerDescriptor, (long) ruleId1 );

        when( checker().ruleAccess.loadSingleSchemaRule( ruleId1 ) ).thenReturn( rule1 );
        when( checker().ruleAccess.loadSingleSchemaRule( ruleId2 ) ).thenReturn( rule2 );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        add( inUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        check( record1 );
        ConsistencyReport.SchemaConsistencyReport report = check( record2 );

        // then
        verify( report ).duplicateObligation( record1 );
    }

    @Test
    public void shouldReportUnreferencedConstraintIndex() throws Exception
    {
        // given
        int ruleId = 0;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record = inUse( dynamicRecord( ruleId ) );

        SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );

        IndexRule rule = IndexRule.constraintIndexRule( ruleId, labelId, propertyKeyId, providerDescriptor, (long) ruleId );

        when( checker().ruleAccess.loadSingleSchemaRule( ruleId ) ).thenReturn( rule );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        add( inUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        check( record );
        SchemaRecordCheck obligationChecker = checker().forObligationChecking();
        ConsistencyReport.SchemaConsistencyReport report = check( obligationChecker, record );

        // then
        verify( report ).missingObligation( SchemaRule.Kind.UNIQUENESS_CONSTRAINT );
    }

    @Test
    public void shouldReportTwoIndexRulesWithDuplicateContent() throws Exception
    {
        // given
        int ruleId1 = 0;
        int ruleId2 = 1;
        int labelId = 1;
        int propertyKeyId = 2;

        DynamicRecord record1 = inUse( dynamicRecord( ruleId1 ) );
        DynamicRecord record2 = inUse( dynamicRecord( ruleId2 ) );

        SchemaIndexProvider.Descriptor providerDescriptor = new SchemaIndexProvider.Descriptor( "in-memory", "1.0" );

        IndexRule rule1 = IndexRule.constraintIndexRule( ruleId1, labelId, propertyKeyId, providerDescriptor, (long) ruleId1 );
        IndexRule rule2 = IndexRule.constraintIndexRule( ruleId2, labelId, propertyKeyId, providerDescriptor, (long) ruleId2 );

        when( checker().ruleAccess.loadSingleSchemaRule( ruleId1 ) ).thenReturn( rule1 );
        when( checker().ruleAccess.loadSingleSchemaRule( ruleId2 ) ).thenReturn( rule2 );

        add( inUse( new LabelTokenRecord( labelId ) ) );
        add( inUse( new PropertyKeyTokenRecord( propertyKeyId ) ) );

        // when
        check( record1 );
        ConsistencyReport.SchemaConsistencyReport report = check( record2 );

        // then
        verify( report ).duplicateRuleContent( record1 );
    }


    private DynamicRecord dynamicRecord( long id )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setType( RecordAccessStub.SCHEMA_RECORD_TYPE );
        return record;
    }
}
