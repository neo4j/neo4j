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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.store.SchemaRuleAccess;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodePropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipPropertyExistenceConstraintRule;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;

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

    private final Map<Long, DynamicRecord> indexObligations;
    private final Map<Long, DynamicRecord> constraintObligations;
    private final Map<SchemaRule, DynamicRecord> verifiedRulesWithRecords;
    private final CheckStrategy strategy;

    public SchemaRecordCheck( SchemaRuleAccess ruleAccess )
    {
        this.ruleAccess = ruleAccess;
        this.indexObligations = new HashMap<>();
        this.constraintObligations = new HashMap<>();
        this.verifiedRulesWithRecords = new HashMap<>();
        this.strategy = new RulesCheckStrategy();
    }

    private SchemaRecordCheck(
            SchemaRuleAccess ruleAccess,
            Map<Long, DynamicRecord> indexObligations,
            Map<Long, DynamicRecord> constraintObligations,
            Map<SchemaRule, DynamicRecord> verifiedRulesWithRecords,
            CheckStrategy strategy )
    {
        this.ruleAccess = ruleAccess;
        this.indexObligations = indexObligations;
        this.constraintObligations = constraintObligations;
        this.verifiedRulesWithRecords = verifiedRulesWithRecords;
        this.strategy = strategy;
    }

    public SchemaRecordCheck forObligationChecking()
    {
        return new SchemaRecordCheck( ruleAccess, indexObligations, constraintObligations, verifiedRulesWithRecords,
                new ObligationsCheckStrategy() );
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

            SchemaRule.Kind kind = rule.getKind();
            switch ( kind )
            {
                case INDEX_RULE:
                case CONSTRAINT_INDEX_RULE:
                    strategy.checkIndexRule( (IndexRule) rule, record, records, engine );
                    break;
                case UNIQUENESS_CONSTRAINT:
                    strategy.checkUniquenessConstraintRule( (UniquePropertyConstraintRule) rule, record, records,
                            engine );
                    break;
                case NODE_PROPERTY_EXISTENCE_CONSTRAINT:
                    strategy.checkNodePropertyExistenceRule( (NodePropertyExistenceConstraintRule) rule, record,
                            records, engine );
                    break;
                case RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT:
                    strategy.checkRelationshipPropertyExistenceRule( (RelationshipPropertyExistenceConstraintRule) rule,
                            record, records, engine );
                    break;
                default:
                    engine.report().unsupportedSchemaRuleKind( kind );
            }
        }
    }

    @Override
    public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord,
            CheckerEngine<DynamicRecord, ConsistencyReport.SchemaConsistencyReport> engine,
            DiffRecordAccess records )
    {
    }

    private interface CheckStrategy
    {
        void checkIndexRule( IndexRule rule, DynamicRecord record, RecordAccess records,
                CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine );

        void checkUniquenessConstraintRule( UniquePropertyConstraintRule rule, DynamicRecord record,
                RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine );

        void checkNodePropertyExistenceRule( NodePropertyExistenceConstraintRule rule, DynamicRecord record,
                RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine );

        void checkRelationshipPropertyExistenceRule( RelationshipPropertyExistenceConstraintRule rule,
                DynamicRecord record, RecordAccess records,
                CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine );
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
            checkLabelAndPropertyRule( rule, rule.getPropertyKey(), record, records, engine );

            if ( rule.isConstraintIndex() && rule.getOwningConstraint() != null )
            {
                DynamicRecord previousObligation = constraintObligations.put( rule.getOwningConstraint(), record );
                if ( previousObligation != null )
                {
                    engine.report().duplicateObligation( previousObligation );
                }
            }
        }

        @Override
        public void checkUniquenessConstraintRule( UniquePropertyConstraintRule rule, DynamicRecord record,
                RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            checkLabelAndPropertyRule( rule, rule.getPropertyKey(), record, records, engine );

            DynamicRecord previousObligation = indexObligations.put( rule.getOwnedIndex(), record );
            if ( previousObligation != null )
            {
                engine.report().duplicateObligation( previousObligation );
            }
        }

        @Override
        public void checkNodePropertyExistenceRule( NodePropertyExistenceConstraintRule rule, DynamicRecord record,
                RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            checkLabelAndPropertyRule( rule, rule.getPropertyKey(), record, records, engine );
        }

        @Override
        public void checkRelationshipPropertyExistenceRule( RelationshipPropertyExistenceConstraintRule rule,
                DynamicRecord record, RecordAccess records,
                CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            checkRelTypeAndPropertyRule( rule, rule.getPropertyKey(), record, records, engine );
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
            if ( rule.isConstraintIndex() )
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
        }

        @Override
        public void checkUniquenessConstraintRule( UniquePropertyConstraintRule rule, DynamicRecord record,
                RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
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

        @Override
        public void checkNodePropertyExistenceRule( NodePropertyExistenceConstraintRule rule, DynamicRecord record,
                RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
        }

        @Override
        public void checkRelationshipPropertyExistenceRule( RelationshipPropertyExistenceConstraintRule rule,
                DynamicRecord record, RecordAccess records,
                CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
        }
    }

    private void checkLabelAndPropertyRule( SchemaRule rule, int propertyKey, DynamicRecord record,
            RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
    {
        engine.comparativeCheck( records.label( rule.getLabel() ), VALID_LABEL );
        engine.comparativeCheck( records.propertyKey( propertyKey ), VALID_PROPERTY_KEY );
        checkForDuplicates( rule, record, engine );
    }

    private void checkRelTypeAndPropertyRule( SchemaRule rule, int propertyKey, DynamicRecord record,
            RecordAccess records, CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
    {
        engine.comparativeCheck( records.relationshipType( rule.getRelationshipType() ), VALID_RELATIONSHIP_TYPE );
        engine.comparativeCheck( records.propertyKey( propertyKey ), VALID_PROPERTY_KEY );
        checkForDuplicates( rule, record, engine );
    }

    private void checkForDuplicates( SchemaRule rule, DynamicRecord record,
            CheckerEngine<DynamicRecord,ConsistencyReport.SchemaConsistencyReport> engine )
    {
        DynamicRecord previousContentRecord = verifiedRulesWithRecords.put( rule, record );
        if ( previousContentRecord != null )
        {
            engine.report().duplicateRuleContent( previousContentRecord );
        }
    }

    private static final ComparativeRecordChecker<DynamicRecord,LabelTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_LABEL =
            new ComparativeRecordChecker<DynamicRecord, LabelTokenRecord, ConsistencyReport.SchemaConsistencyReport>()
    {
        @Override
        public void checkReference( DynamicRecord record, LabelTokenRecord labelTokenRecord,
                                    CheckerEngine<DynamicRecord, ConsistencyReport.SchemaConsistencyReport> engine,
                                    RecordAccess records )
        {
            if ( !labelTokenRecord.inUse() )
            {
                engine.report().labelNotInUse( labelTokenRecord );
            }
        }
    };

    private static final ComparativeRecordChecker<DynamicRecord,RelationshipTypeTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_RELATIONSHIP_TYPE =
            new ComparativeRecordChecker<DynamicRecord, RelationshipTypeTokenRecord,
                    ConsistencyReport.SchemaConsistencyReport>()
    {
        @Override
        public void checkReference( DynamicRecord record, RelationshipTypeTokenRecord relTypeTokenRecord,
                                    CheckerEngine<DynamicRecord, ConsistencyReport.SchemaConsistencyReport> engine,
                                    RecordAccess records )
        {
            if ( !relTypeTokenRecord.inUse() )
            {
                engine.report().relationshipTypeNotInUse( relTypeTokenRecord );
            }
        }
    };

    private static final ComparativeRecordChecker<DynamicRecord, PropertyKeyTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_PROPERTY_KEY =
            new ComparativeRecordChecker<DynamicRecord, PropertyKeyTokenRecord, ConsistencyReport.SchemaConsistencyReport>()
    {
        @Override
        public void checkReference( DynamicRecord record, PropertyKeyTokenRecord propertyKeyTokenRecord,
                                    CheckerEngine<DynamicRecord, ConsistencyReport.SchemaConsistencyReport> engine,
                                    RecordAccess records )
        {
            if ( !propertyKeyTokenRecord.inUse() )
            {
                engine.report().propertyKeyNotInUse( propertyKeyTokenRecord );
            }
        }
    };
}
