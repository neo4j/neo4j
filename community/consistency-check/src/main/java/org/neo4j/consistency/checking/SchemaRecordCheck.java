/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.store.SchemaRuleAccess;
import org.neo4j.kernel.impl.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;

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
    enum Phase
    {
        /**
         * Verify rules can be de-serialized, have valid forward references, and build up internal state
         * for checking in back references in later phases (obligations)
         */
        CHECK_RULES,

        /** Verify obligations, that is correct back references */
        CHECK_OBLIGATIONS
    }

    final SchemaRuleAccess ruleAccess;
    final Map<Long, DynamicRecord> indexObligations;
    final Map<Long, DynamicRecord> constraintObligations;
    final Map<SchemaRuleContent, DynamicRecord> contentMap;
    final Phase phase;

    private SchemaRecordCheck(
            SchemaRuleAccess ruleAccess,
            Map<Long, DynamicRecord> indexObligations,
            Map<Long, DynamicRecord> constraintObligations,
            Map<SchemaRuleContent, DynamicRecord> contentMap,
            Phase phase )
    {
        this.ruleAccess = ruleAccess;
        this.indexObligations = indexObligations;
        this.constraintObligations = constraintObligations;
        this.contentMap = contentMap;
        this.phase = phase;
    }

    public SchemaRecordCheck( SchemaRuleAccess ruleAccess )
    {
        this( ruleAccess,
              new HashMap<Long, DynamicRecord>(),
              new HashMap<Long, DynamicRecord>(),
              new HashMap<SchemaRuleContent, DynamicRecord>(),
              Phase.CHECK_RULES );
    }

    public SchemaRecordCheck forObligationChecking()
    {
        return new SchemaRecordCheck(
                ruleAccess, indexObligations, constraintObligations, contentMap, Phase.CHECK_OBLIGATIONS );
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

            // given a parsed schema rule
            if ( Phase.CHECK_RULES == phase )
            {
                engine.comparativeCheck( records.label( rule.getLabel() ), VALID_LABEL );

                SchemaRuleContent content = new SchemaRuleContent( rule );
                DynamicRecord previousContentRecord = contentMap.put( content, record );
                if ( null != previousContentRecord )
                {
                    engine.report().duplicateRuleContent( previousContentRecord );
                }
            }

            SchemaRule.Kind kind = rule.getKind();
            switch ( kind )
            {
                case INDEX_RULE:
                case CONSTRAINT_INDEX_RULE:
                    checkIndexRule( (IndexRule) rule, engine, record, records );
                    break;
                case UNIQUENESS_CONSTRAINT:
                    checkUniquenessConstraintRule( (UniquenessConstraintRule) rule, engine, record, records );
                    break;
                default:
                    engine.report().unsupportedSchemaRuleKind( kind );
            }
        }
    }

    private void checkUniquenessConstraintRule( UniquenessConstraintRule rule,
                                                CheckerEngine<DynamicRecord, ConsistencyReport.SchemaConsistencyReport> engine,
                                                DynamicRecord record, RecordAccess records )
    {
        if ( phase == Phase.CHECK_RULES )
        {
            engine.comparativeCheck( records.propertyKey( rule.getPropertyKey() ), VALID_PROPERTY_KEY );
            DynamicRecord previousObligation = indexObligations.put( rule.getOwnedIndex(), record );
            if ( null != previousObligation )
            {
                engine.report().duplicateObligation( previousObligation );
            }
        }
        else if ( phase == Phase.CHECK_OBLIGATIONS )
        {
            DynamicRecord obligation = constraintObligations.get( rule.getId() );
            if ( null == obligation)
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

    private void checkIndexRule( IndexRule rule,
                                 CheckerEngine<DynamicRecord, ConsistencyReport.SchemaConsistencyReport> engine,
                                 DynamicRecord record, RecordAccess records )
    {
        if ( phase == Phase.CHECK_RULES )
        {
            engine.comparativeCheck( records.propertyKey( rule.getPropertyKey() ), VALID_PROPERTY_KEY );
            if ( rule.isConstraintIndex() && rule.getOwningConstraint() != null )
            {
                DynamicRecord previousObligation = constraintObligations.put( rule.getOwningConstraint(), record );
                if ( null != previousObligation )
                {
                    engine.report().duplicateObligation( previousObligation );
                }
            }
        }
        else if ( phase == Phase.CHECK_OBLIGATIONS )
        {
            if ( rule.isConstraintIndex() )
            {
                DynamicRecord obligation = indexObligations.get( rule.getId() );
                if ( null == obligation ) // no pointer to here
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
    }



    @Override
    public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord,
                             CheckerEngine<DynamicRecord, ConsistencyReport.SchemaConsistencyReport> engine,
                             DiffRecordAccess records )
    {
    }

    public static final ComparativeRecordChecker<DynamicRecord,LabelTokenRecord,
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

    public static final ComparativeRecordChecker<DynamicRecord, PropertyKeyTokenRecord,
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
