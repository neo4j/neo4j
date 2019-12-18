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
import java.util.OptionalLong;

import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaProcessor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

/**
 * Note that this class builds up an in-memory representation of the complete schema store by being used in
 * multiple phases.
 *
 * This differs from other store checks, where we deliberately avoid building up state, expecting store to generally be
 * larger than available memory. However, it is safe to make the assumption that schema storage will fit in memory
 * because the same assumption is also made by the online database.
 */
public class SchemaRecordCheck implements RecordCheck<SchemaRecord, ConsistencyReport.SchemaConsistencyReport>
{
    static final String CONSTRAINT_OBLIGATION = "UNIQUENESS_CONSTRAINT";
    static final String INDEX_OBLIGATION = "CONSTRAINT_INDEX_RULE";

    final SchemaRuleAccess ruleAccess;
    final IndexAccessors indexAccessors;

    private final Map<Long, SchemaRecord> indexObligations;
    private final Map<Long, String> indexNameObligations;
    private final Map<Long, SchemaRecord> constraintObligations;
    private final Map<SchemaRuleKey, SchemaRecord> verifiedRulesWithRecords;
    private final Map<String, NamedSchema> verifiedRuleNames;
    private final CheckStrategy strategy;

    public SchemaRecordCheck( SchemaRuleAccess ruleAccess, IndexAccessors indexAccessors )
    {
        this.ruleAccess = ruleAccess;
        this.indexAccessors = indexAccessors;
        this.indexObligations = new HashMap<>();
        this.indexNameObligations = new HashMap<>();
        this.constraintObligations = new HashMap<>();
        this.verifiedRulesWithRecords = new HashMap<>();
        this.verifiedRuleNames = new HashMap<>();
        this.strategy = new RulesCheckStrategy();
    }

    private SchemaRecordCheck(
            SchemaRuleAccess ruleAccess,
            IndexAccessors indexAccessors,
            Map<Long, SchemaRecord> indexObligations,
            Map<Long, String> indexNameObligations,
            Map<Long, SchemaRecord> constraintObligations,
            Map<SchemaRuleKey, SchemaRecord> verifiedRulesWithRecords,
            Map<String, NamedSchema> verifiedRuleNames,
            CheckStrategy strategy )
    {
        this.ruleAccess = ruleAccess;
        this.indexAccessors = indexAccessors;
        this.indexObligations = indexObligations;
        this.indexNameObligations = indexNameObligations;
        this.constraintObligations = constraintObligations;
        this.verifiedRulesWithRecords = verifiedRulesWithRecords;
        this.verifiedRuleNames = verifiedRuleNames;
        this.strategy = strategy;
    }

    public SchemaRecordCheck forObligationChecking()
    {
        return new SchemaRecordCheck( ruleAccess, indexAccessors, indexObligations, indexNameObligations, constraintObligations,
                verifiedRulesWithRecords, verifiedRuleNames, new ObligationsCheckStrategy() );
    }

    @Override
    public void check( SchemaRecord record,
                       CheckerEngine<SchemaRecord, ConsistencyReport.SchemaConsistencyReport> engine,
                       RecordAccess records )
    {
        if ( record.inUse() )
        {
            // parse schema rule
            SchemaRule rule;
            try
            {
                rule = ruleAccess.loadSingleSchemaRule( record.getId() );
            }
            catch ( MalformedSchemaRuleException e )
            {
                strategy.reportMalformedSchemaRule( engine.report() );
                return;
            }

            if ( rule instanceof IndexDescriptor )
            {
                strategy.checkIndexRule( (IndexDescriptor)rule, record, records, engine );
            }
            else if ( rule instanceof ConstraintDescriptor )
            {
                strategy.checkConstraintRule( (ConstraintDescriptor) rule, record, records, engine );
            }
            else
            {
                engine.report().unsupportedSchemaRuleType( rule.getClass() );
            }
        }
    }

    private interface CheckStrategy
    {
        void checkIndexRule( IndexDescriptor rule, SchemaRecord record, RecordAccess records,
                             CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine );

        void checkConstraintRule( ConstraintDescriptor rule, SchemaRecord record,
                RecordAccess records, CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine );

        void reportMalformedSchemaRule( ConsistencyReport.SchemaConsistencyReport report );
    }

    /**
     * Verify rules can be de-serialized, have valid forward references, and build up internal state
     * for checking in back references in later phases (obligations)
     */
    private class RulesCheckStrategy implements CheckStrategy
    {
        @Override
        public void checkIndexRule( IndexDescriptor rule, SchemaRecord record, RecordAccess records,
                                    CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            checkSchema( rule, record, records, engine );

            if ( rule.isUnique() && rule.getOwningConstraintId().isPresent() )
            {
                SchemaRecord previousObligation = constraintObligations.put( rule.getOwningConstraintId().getAsLong(), record.copy() );
                if ( previousObligation != null )
                {
                    engine.report().duplicateObligation( previousObligation );
                }
            }
        }

        @Override
        public void checkConstraintRule( ConstraintDescriptor constraint, SchemaRecord record,
                RecordAccess records, CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            checkSchema( constraint, record, records, engine );

            if ( constraint.isIndexBackedConstraint() )
            {
                IndexBackedConstraintDescriptor indexBacked = constraint.asIndexBackedConstraint();
                SchemaRecord previousObligation = indexObligations.put( indexBacked.ownedIndexId(), record.copy() );
                if ( previousObligation != null )
                {
                    engine.report().duplicateObligation( previousObligation );
                }
                indexNameObligations.put( indexBacked.ownedIndexId(), indexBacked.getName() );
            }
        }

        @Override
        public void reportMalformedSchemaRule( ConsistencyReport.SchemaConsistencyReport report )
        {
            report.malformedSchemaRule();
        }
    }

    /**
     * Verify obligations, that is correct back references
     */
    private class ObligationsCheckStrategy implements CheckStrategy
    {
        @Override
        public void checkIndexRule( IndexDescriptor rule, SchemaRecord record, RecordAccess records,
                                    CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            if ( rule.isUnique() )
            {
                SchemaRecord obligation = indexObligations.get( rule.getId() );
                if ( obligation == null ) // no pointer to here
                {
                    if ( rule.getOwningConstraintId().isPresent() ) // we only expect a pointer if we have an owner
                    {
                        engine.report().missingObligation( CONSTRAINT_OBLIGATION );
                    }
                }
                else
                {
                    // if someone points to here, it must be our owner
                    if ( obligation.getId() != rule.getOwningConstraintId().getAsLong() )
                    {
                        engine.report().constraintIndexRuleNotReferencingBack( obligation );
                    }
                }

                String nameObligation = indexNameObligations.get( rule.getId() );
                if ( nameObligation != null && !nameObligation.equals( rule.getName() ) )
                {
                    engine.report().constraintIndexNameDoesNotMatchConstraintName( record, rule.getName(), nameObligation );
                }
            }
            if ( indexAccessors.notOnlineRules().contains( rule ) )
            {
                engine.report().schemaRuleNotOnline( rule );
            }
        }

        @Override
        public void checkConstraintRule( ConstraintDescriptor constraint, SchemaRecord record,
                RecordAccess records, CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine )
        {
            if ( constraint.isIndexBackedConstraint() )
            {
                SchemaRecord obligation = constraintObligations.get( constraint.getId() );
                if ( obligation == null )
                {
                    engine.report().missingObligation( INDEX_OBLIGATION );
                }
                else
                {
                    if ( obligation.getId() != constraint.asIndexBackedConstraint().ownedIndexId() )
                    {
                        engine.report().uniquenessConstraintNotReferencingBack( obligation );
                    }
                }
            }
        }

        @Override
        public void reportMalformedSchemaRule( ConsistencyReport.SchemaConsistencyReport report )
        {
            // Do nothing. The RulesCheckStrategy will report this for us.
        }
    }

    private void checkSchema( SchemaRule rule, SchemaRecord record,
            RecordAccess records, CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine )
    {
        rule.schema().processWith( new CheckSchema( engine, records ) );
        checkNamesAndDuplicates( rule, record, engine );
    }

    static class CheckSchema implements SchemaProcessor
    {
        private final CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine;
        private final RecordAccess records;

        CheckSchema( CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine,
                RecordAccess records )
        {
            this.engine = engine;
            this.records = records;
        }

        @Override
        public void processSpecific( LabelSchemaDescriptor schema )
        {
            engine.comparativeCheck( records.label( schema.getLabelId() ), VALID_LABEL );
            checkProperties( schema.getPropertyIds() );
        }

        @Override
        public void processSpecific( RelationTypeSchemaDescriptor schema )
        {
            engine.comparativeCheck( records.relationshipType( schema.getRelTypeId() ), VALID_RELATIONSHIP_TYPE );
            checkProperties( schema.getPropertyIds() );
        }

        @Override
        public void processSpecific( SchemaDescriptor schema )
        {
            switch ( schema.entityType() )
            {
            case NODE:
                for ( int entityTokenId : schema.getEntityTokenIds() )
                {
                    engine.comparativeCheck( records.label( entityTokenId ), VALID_LABEL );
                }
                break;
            case RELATIONSHIP:
                for ( int entityTokenId : schema.getEntityTokenIds() )
                {
                    engine.comparativeCheck( records.relationshipType( entityTokenId ), VALID_RELATIONSHIP_TYPE );
                }
                break;
            default:
                throw new IllegalArgumentException( "Schema with given entity type is not supported: " + schema.entityType() );
            }

            checkProperties( schema.getPropertyIds() );
        }

        private void checkProperties( int[] propertyIds )
        {
            for ( int propertyId : propertyIds )
            {
                engine.comparativeCheck( records.propertyKey( propertyId ), VALID_PROPERTY_KEY );
            }
        }
    }

    private void checkNamesAndDuplicates( SchemaRule rule, SchemaRecord record,
            CheckerEngine<SchemaRecord,ConsistencyReport.SchemaConsistencyReport> engine )
    {
        SchemaRecord previousContentRecord = verifiedRulesWithRecords.put( new SchemaRuleKey( rule ), record.copy() );
        if ( previousContentRecord != null )
        {
            engine.report().duplicateRuleContent( previousContentRecord );
        }

        String name = rule.getName();
        NamedSchema namedSchema = verifiedRuleNames.get( name );
        if ( namedSchema == null )
        {
            namedSchema = new NamedSchema();
            verifiedRuleNames.put( name, namedSchema );
        }
        if ( rule instanceof ConstraintDescriptor )
        {
            ConstraintDescriptor constraint = (ConstraintDescriptor) rule;
            if ( namedSchema.constraint != null )
            {
                engine.report().duplicateRuleName( namedSchema.constraintRecord, name );
            }
            namedSchema.constraint = constraint;
            namedSchema.constraintRecord = record;
            if ( namedSchema.index != null )
            {
                if ( constraint.isIndexBackedConstraint() )
                {
                    IndexBackedConstraintDescriptor ibc = constraint.asIndexBackedConstraint();
                    if ( ibc.hasOwnedIndexId() && ibc.ownedIndexId() == namedSchema.index.getId() )
                    {
                        return;
                    }
                }
                if ( namedSchema.indexRecord.getId() != rule.getId() /*don't report itself*/ )
                {
                    engine.report().duplicateRuleName( namedSchema.indexRecord, name );
                }
            }
        }
        else
        {
            IndexDescriptor index = (IndexDescriptor) rule;
            if ( namedSchema.index != null &&
                    namedSchema.indexRecord.getId() != index.getId() /*don't report itself*/ )
            {
                engine.report().duplicateRuleName( namedSchema.indexRecord, name );
            }
            namedSchema.index = index;
            namedSchema.indexRecord = record;
            if ( namedSchema.constraint != null )
            {
                OptionalLong owningConstraintId = index.getOwningConstraintId();
                if ( owningConstraintId.isEmpty() || owningConstraintId.getAsLong() != namedSchema.constraint.getId() )
                {
                    engine.report().duplicateRuleName( namedSchema.constraintRecord, name );
                }
            }
        }
    }

    private static final ComparativeRecordChecker<SchemaRecord,LabelTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_LABEL =
            ( record, labelTokenRecord, engine, records ) ->
            {
                if ( !labelTokenRecord.inUse() )
                {
                    engine.report().labelNotInUse( labelTokenRecord );
                }
            };

    private static final ComparativeRecordChecker<SchemaRecord,RelationshipTypeTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_RELATIONSHIP_TYPE =
            ( record, relTypeTokenRecord, engine, records ) ->
            {
                if ( !relTypeTokenRecord.inUse() )
                {
                    engine.report().relationshipTypeNotInUse( relTypeTokenRecord );
                }
            };

    private static final ComparativeRecordChecker<SchemaRecord, PropertyKeyTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_PROPERTY_KEY =
            ( record, propertyKeyTokenRecord, engine, records ) ->
            {
                if ( !propertyKeyTokenRecord.inUse() )
                {
                    engine.report().propertyKeyNotInUse( propertyKeyTokenRecord );
                }
            };

    private static class NamedSchema
    {
        private IndexDescriptor index;
        private ConstraintDescriptor constraint;
        private SchemaRecord indexRecord;
        private SchemaRecord constraintRecord;
    }
}
