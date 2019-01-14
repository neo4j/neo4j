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
package org.neo4j.consistency.checking;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaProcessor;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.store.SchemaRuleAccess;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.storageengine.api.schema.SchemaRule;

/**
 * Note that this class builds up an in-memory representation of the complete schema store by being used in
 * multiple phases.
 *
 * This differs from other store checks, where we deliberately avoid building up state, expecting store to generally be
 * larger than available memory. However, it is safe to make the assumption that schema storage will fit in memory
 * because the same assumption is also made by the online database.
 */
public class SchemaRecordCheck implements RecordCheck<DynamicRecord, ConsistencyReport.SchemaConsistencyReport>
{
    final SchemaRuleAccess ruleAccess;

    private final IndexAccessors indexAccessors;
    private final Map<Long, DynamicRecord> indexObligations;
    private final Map<Long, DynamicRecord> constraintObligations;
    private final Map<SchemaRule, DynamicRecord> verifiedRulesWithRecords;
    private final CheckStrategy strategy;

    public SchemaRecordCheck( SchemaRuleAccess ruleAccess, IndexAccessors indexAccessors )
    {
        this.ruleAccess = ruleAccess;
        this.indexAccessors = indexAccessors;
        this.indexObligations = new HashMap<>();
        this.constraintObligations = new HashMap<>();
        this.verifiedRulesWithRecords = new HashMap<>();
        this.strategy = new RulesCheckStrategy();
    }

    private SchemaRecordCheck(
            SchemaRuleAccess ruleAccess,
            IndexAccessors indexAccessors,
            Map<Long, DynamicRecord> indexObligations,
            Map<Long, DynamicRecord> constraintObligations,
            Map<SchemaRule, DynamicRecord> verifiedRulesWithRecords,
            CheckStrategy strategy )
    {
        this.ruleAccess = ruleAccess;
        this.indexAccessors = indexAccessors;
        this.indexObligations = indexObligations;
        this.constraintObligations = constraintObligations;
        this.verifiedRulesWithRecords = verifiedRulesWithRecords;
        this.strategy = strategy;
    }

    public SchemaRecordCheck forObligationChecking()
    {
        return new SchemaRecordCheck( ruleAccess, indexAccessors, indexObligations, constraintObligations,
                verifiedRulesWithRecords, new ObligationsCheckStrategy() );
    }

    @Override
    public void check( DynamicRecord record,
                       CheckerEngine<DynamicRecord, ConsistencyReport.SchemaConsistencyReport> engine,
                       RecordAccess records )
    {
        if ( record.inUse() && record.isStartRecord() )
        {
            // parse schema rule
            SchemaRule rule;
            try
            {
                rule = ruleAccess.loadSingleSchemaRule( record.getId() );
            }
            catch ( MalformedSchemaRuleException e )
            {
                engine.report().malformedSchemaRule();
                return;
            }

            if ( rule instanceof IndexRule )
            {
                strategy.checkIndexRule( (IndexRule)rule, record, records, engine );
            }
            else if ( rule instanceof ConstraintRule )
            {
                strategy.checkConstraintRule( (ConstraintRule) rule, record, records, engine );
            }
            else
            {
                engine.report().unsupportedSchemaRuleKind( null );
            }
        }
    }

    private interface CheckStrategy
    {
        void checkIndexRule( IndexRule rule, DynamicRecord record, RecordAccess records,
                CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine );

        void checkConstraintRule( ConstraintRule rule, DynamicRecord record,
                RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine );
    }

    /**
     * Verify rules can be de-serialized, have valid forward references, and build up internal state
     * for checking in back references in later phases (obligations)
     */
    private class RulesCheckStrategy implements CheckStrategy
    {
        @Override
        public void checkIndexRule( IndexRule rule, DynamicRecord record, RecordAccess records,
                CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            checkSchema( rule, record, records, engine );

            if ( rule.canSupportUniqueConstraint() && rule.getOwningConstraint() != null )
            {
                DynamicRecord previousObligation = constraintObligations.put( rule.getOwningConstraint(), record.clone() );
                if ( previousObligation != null )
                {
                    engine.report().duplicateObligation( previousObligation );
                }
            }
        }

        @Override
        public void checkConstraintRule( ConstraintRule rule, DynamicRecord record,
                RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            checkSchema( rule, record, records, engine );

            if ( rule.getConstraintDescriptor().enforcesUniqueness() )
            {
                DynamicRecord previousObligation = indexObligations.put( rule.getOwnedIndex(), record.clone() );
                if ( previousObligation != null )
                {
                    engine.report().duplicateObligation( previousObligation );
                }
            }
        }
    }

    /**
     * Verify obligations, that is correct back references
     */
    private class ObligationsCheckStrategy implements CheckStrategy
    {
        @Override
        public void checkIndexRule( IndexRule rule, DynamicRecord record, RecordAccess records,
                CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            if ( rule.canSupportUniqueConstraint() )
            {
                DynamicRecord obligation = indexObligations.get( rule.getId() );
                if ( obligation == null ) // no pointer to here
                {
                    if ( rule.getOwningConstraint() != null ) // we only expect a pointer if we have an owner
                    {
                        engine.report().missingObligation( SchemaRule.Kind.UNIQUENESS_CONSTRAINT );
                    }
                }
                else
                {
                    // if someone points to here, it must be our owner
                    if ( obligation.getId() != rule.getOwningConstraint() )
                    {
                        engine.report().constraintIndexRuleNotReferencingBack( obligation );
                    }
                }
            }
            if ( indexAccessors.notOnlineRules().contains( rule ) )
            {
                engine.report().schemaRuleNotOnline( rule );
            }
        }

        @Override
        public void checkConstraintRule( ConstraintRule rule, DynamicRecord record,
                RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            if ( rule.getConstraintDescriptor().enforcesUniqueness() )
            {
                DynamicRecord obligation = constraintObligations.get( rule.getId() );
                if ( obligation == null )
                {
                    engine.report().missingObligation( SchemaRule.Kind.CONSTRAINT_INDEX_RULE );
                }
                else
                {
                    if ( obligation.getId() != rule.getOwnedIndex() )
                    {
                        engine.report().uniquenessConstraintNotReferencingBack( obligation );
                    }
                }
            }
        }
    }

    private void checkSchema( SchemaRule rule, DynamicRecord record,
            RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
    {
        rule.schema().processWith( new CheckSchema( engine, records ) );
        checkForDuplicates( rule, record, engine );
    }

    static class CheckSchema implements SchemaProcessor
    {
        private final CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine;
        private final RecordAccess records;

        CheckSchema( CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine,
                RecordAccess records )
        {
            this.engine = engine;
            this.records = records;
        }

        @Override
        public void processSpecific( LabelSchemaDescriptor schema )
        {
            engine.comparativeCheck( records.label( schema.getLabelId() ), VALID_LABEL );
            for ( int propertyId : schema.getPropertyIds() )
            {
                engine.comparativeCheck( records.propertyKey( propertyId ), VALID_PROPERTY_KEY );
            }
        }

        @Override
        public void processSpecific( RelationTypeSchemaDescriptor schema )
        {
            engine.comparativeCheck( records.relationshipType( schema.getRelTypeId() ), VALID_RELATIONSHIP_TYPE );
            for ( int propertyId : schema.getPropertyIds() )
            {
                engine.comparativeCheck( records.propertyKey( propertyId ), VALID_PROPERTY_KEY );
            }
        }
    }

    private void checkForDuplicates( SchemaRule rule, DynamicRecord record,
            CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
    {
        DynamicRecord previousContentRecord = verifiedRulesWithRecords.put( rule, record.clone() );
        if ( previousContentRecord != null )
        {
            engine.report().duplicateRuleContent( previousContentRecord );
        }
    }

    private static final ComparativeRecordChecker<DynamicRecord,LabelTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_LABEL =
            ( record, labelTokenRecord, engine, records ) ->
            {
                if ( !labelTokenRecord.inUse() )
                {
                    engine.report().labelNotInUse( labelTokenRecord );
                }
            };

    private static final ComparativeRecordChecker<DynamicRecord,RelationshipTypeTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_RELATIONSHIP_TYPE =
            ( record, relTypeTokenRecord, engine, records ) ->
            {
                if ( !relTypeTokenRecord.inUse() )
                {
                    engine.report().relationshipTypeNotInUse( relTypeTokenRecord );
                }
            };

    private static final ComparativeRecordChecker<DynamicRecord, PropertyKeyTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_PROPERTY_KEY =
            ( record, propertyKeyTokenRecord, engine, records ) ->
            {
                if ( !propertyKeyTokenRecord.inUse() )
                {
                    engine.report().propertyKeyNotInUse( propertyKeyTokenRecord );
                }
            };
}
