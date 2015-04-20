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
package org.neo4j.consistency.checking.incremental;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.SchemaRecordCheck;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.store.DiffStore;
import org.neo4j.consistency.store.DirectDiffRecordAccess;
import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaRuleAccess;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.kernel.impl.store.RecordStore.Scanner.scan;

public class IncrementalDiffCheck extends DiffCheck
{
    public IncrementalDiffCheck( StringLogger logger )
    {
        super( logger );
    }

    @Override
    public ConsistencySummaryStatistics execute( DiffStore diffs )
    {
        ConsistencySummaryStatistics summary = new ConsistencySummaryStatistics();
        ConsistencyReporter reporter = new ConsistencyReporter( new DirectDiffRecordAccess( diffs ),
                new InconsistencyReport( new InconsistencyMessageLogger( logger ), summary ) );
        StoreProcessor processor = new StoreProcessor( reporter );
        diffs.applyToAll( processor );

        if ( diffs.getSchemaStore().hasChanges() )
        {
            if ( 0 == summary.getInconsistencyCountForRecordType( RecordType.SCHEMA ) )
            {
                performFullCheckOfSchemaStore( diffs, reporter );
            }
        }

        return summary;
    }

    @SuppressWarnings("unchecked")
    private void performFullCheckOfSchemaStore( DiffStore diffs, ConsistencyReporter reporter )
    {
        SchemaRecordCheck schemaChecker = new SchemaRecordCheck( new RuleReader( diffs.getSchemaStore() ) );
        for ( DynamicRecord record : scan( diffs.getSchemaStore() ) )
        {
            reporter.forSchema( record, schemaChecker );
        }
        schemaChecker = schemaChecker.forObligationChecking();
        for ( DynamicRecord record : scan( diffs.getSchemaStore() ) )
        {
            reporter.forSchema( record, schemaChecker );
        }
    }

    private static class RuleReader implements SchemaRuleAccess
    {
        private final RecordStore<DynamicRecord> schemaStore;

        private RuleReader( RecordStore<DynamicRecord> schemaStore )
        {
            this.schemaStore = schemaStore;
        }

        @Override
        public SchemaRule loadSingleSchemaRule( long ruleId ) throws MalformedSchemaRuleException
        {
            return SchemaStore.readSchemaRule( ruleId, schemaStore.getRecords( ruleId ) );
        }
    }
}
