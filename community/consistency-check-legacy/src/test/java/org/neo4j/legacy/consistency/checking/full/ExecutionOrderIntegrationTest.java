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
package org.neo4j.legacy.consistency.checking.full;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.legacy.consistency.ConsistencyCheckSettings;
import org.neo4j.legacy.consistency.checking.CheckDecorator;
import org.neo4j.legacy.consistency.checking.CheckerEngine;
import org.neo4j.legacy.consistency.checking.ComparativeRecordChecker;
import org.neo4j.legacy.consistency.checking.GraphStoreFixture;
import org.neo4j.legacy.consistency.checking.OwningRecordCheck;
import org.neo4j.legacy.consistency.checking.RecordCheck;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.legacy.consistency.report.InconsistencyLogger;
import org.neo4j.legacy.consistency.report.InconsistencyReport;
import org.neo4j.legacy.consistency.report.PendingReferenceCheck;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;
import org.neo4j.legacy.consistency.store.RecordReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.withSettings;

import static java.lang.String.format;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

public class ExecutionOrderIntegrationTest
{
    @Rule
    public final GraphStoreFixture fixture = new GraphStoreFixture()
    {
        @Override
        protected void generateInitialData( GraphDatabaseService graphDb )
        {
            // TODO: create bigger sample graph here
            try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
            {
                Node node1 = set( graphDb.createNode() );
                Node node2 = set( graphDb.createNode(), property( "key", "value" ) );
                node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "C" ) );
                tx.success();
            }
        }
    };
    private static final boolean LOG_DUPLICATES = false;

    @Test
    public void shouldRunSameChecksInMultiPassAsInSingleSingleThreadedPass() throws Exception
    {
        // given
        StoreAccess store = fixture.directStoreAccess().nativeStores();
        DiffRecordAccess access = FullCheck.recordAccess( store );

        FullCheck singlePass = new FullCheck( config( TaskExecutionOrder.SINGLE_THREADED ),
                ProgressMonitorFactory.NONE );
        FullCheck multiPass = new FullCheck( config( TaskExecutionOrder.MULTI_PASS ),
                ProgressMonitorFactory.NONE );

        ConsistencySummaryStatistics multiPassSummary = new ConsistencySummaryStatistics();
        ConsistencySummaryStatistics singlePassSummary = new ConsistencySummaryStatistics();
        InconsistencyLogger logger = mock( InconsistencyLogger.class );
        InvocationLog singlePassChecks = new InvocationLog();
        InvocationLog multiPassChecks = new InvocationLog();

        // when
        singlePass.execute( fixture.directStoreAccess(), new LogDecorator( singlePassChecks ), access, new InconsistencyReport( logger,
                singlePassSummary ) );

        multiPass.execute( fixture.directStoreAccess(), new LogDecorator( multiPassChecks ), access, new InconsistencyReport( logger,
                multiPassSummary ) );

        // then
        verifyZeroInteractions( logger );
        assertEquals( "Expected no inconsistencies in single pass.",
                0, singlePassSummary.getTotalInconsistencyCount() );
        assertEquals( "Expected no inconsistencies in multiple passes.",
                0, multiPassSummary.getTotalInconsistencyCount() );

        assertSameChecks( singlePassChecks.data, multiPassChecks.data );

        if ( singlePassChecks.duplicates.size() != multiPassChecks.duplicates.size() )
        {
            if ( LOG_DUPLICATES )
            {
                throw new Exception(
                        format( "Duplicate checks with single pass: %s, duplicate checks with multiple passes: %s%n",
                                singlePassChecks.duplicates, multiPassChecks.duplicates ) );
            }
        }
    }

    static Config config( TaskExecutionOrder executionOrder )
    {
        Map<String,String> params = stringMap(
                ConsistencyCheckSettings.consistency_check_execution_order.name(), executionOrder.name(),
                // Enable property owners check by default in tests:
                ConsistencyCheckSettings.consistency_check_property_owners.name(), "true" );
        return new Config( params, GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
    }

    private static class InvocationLog
    {
        private final Map<String, Throwable> data = new HashMap<>();
        private final Map<String, Integer> duplicates = new HashMap<>();

        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        void log( PendingReferenceCheck<?> check, InvocationOnMock invocation )
        {
            Method method = invocation.getMethod();
            if ( Object.class == method.getDeclaringClass() && "finalize".equals( method.getName() ) )
            {
                /* skip invocations to finalize - they are not of interest to us,
                 * and GC is not predictable enough to reliably trace this. */
                return;
            }
            StringBuilder entry = new StringBuilder( method.getName() ).append( '(' );
            entry.append( check );
            for ( Object arg : invocation.getArguments() )
            {
                if ( arg instanceof AbstractBaseRecord )
                {
                    AbstractBaseRecord record = (AbstractBaseRecord) arg;
                    entry.append( ',' ).append( record.getClass().getSimpleName() )
                            .append( '[' ).append( record.getLongId() ).append( ']' );
                }
            }
            String message = entry.append( ')' ).toString();
            if ( null != data.put( message, new Throwable( message ) ) )
            {
                Integer cur = duplicates.get( message );
                if ( cur == null )
                {
                    cur = 1;
                }
                duplicates.put( message, cur + 1 );
            }
        }
    }

    private static void assertSameChecks( Map<String, Throwable> singlePassChecks,
                                          Map<String, Throwable> multiPassChecks )
    {
        if ( !singlePassChecks.keySet().equals( multiPassChecks.keySet() ) )
        {
            Map<String, Throwable> missing = new HashMap<>( singlePassChecks );
            Map<String, Throwable> extras = new HashMap<>( multiPassChecks );
            missing.keySet().removeAll( multiPassChecks.keySet() );
            extras.keySet().removeAll( singlePassChecks.keySet() );

            StringBuilder headers = new StringBuilder("\n");
            StringWriter diff = new StringWriter();
            PrintWriter writer = new PrintWriter( diff );
            if ( !missing.isEmpty() )
            {
                writer.append( "These expected checks were missing:\n" );
                for ( Map.Entry<String, Throwable> check : missing.entrySet() )
                {
                    writer.append( "  " );
                    headers.append( "Missing: " ).append( check.getKey() ).append( "\n" );
                    check.getValue().printStackTrace( writer );
                }
            }
            if ( !extras.isEmpty() )
            {
                writer.append( "These extra checks were not expected:\n" );
                for ( Map.Entry<String, Throwable> check : extras.entrySet() )
                {
                    writer.append( "  " );
                    headers.append( "Unexpected: " ).append( check.getKey() ).append( "\n" );
                    check.getValue().printStackTrace( writer );
                }
            }
            fail( headers.toString() + diff.toString() );
        }
    }

    private static class LogDecorator implements CheckDecorator
    {
        private final InvocationLog log;

        LogDecorator( InvocationLog log )
        {
            this.log = log;
        }

        <REC extends AbstractBaseRecord, REP extends ConsistencyReport> OwningRecordCheck<REC, REP> logging(
                RecordCheck<REC, REP> checker )
        {
            return new LoggingChecker<>( checker, log );
        }

        @Override
        public OwningRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
                OwningRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public OwningRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
                OwningRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public OwningRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
                OwningRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
                RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> decoratePropertyKeyTokenChecker(
                RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> decorateRelationshipTypeTokenChecker(
                RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> decorateLabelTokenChecker(
                RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> decorateLabelMatchChecker(
                RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> decorateRelationshipGroupChecker(
                RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> checker )
        {
            return logging( checker );
        }
    }

    private static class LoggingChecker<REC extends AbstractBaseRecord, REP extends ConsistencyReport>
            implements OwningRecordCheck<REC, REP>
    {
        private final RecordCheck<REC, REP> checker;
        private final InvocationLog log;

        LoggingChecker( RecordCheck<REC, REP> checker, InvocationLog log )
        {
            this.checker = checker;
            this.log = log;
        }

        @Override
        public void check( REC record, CheckerEngine<REC, REP> engine, RecordAccess records )
        {
            checker.check( record, engine, new ComparativeLogging( (DiffRecordAccess) records, log ) );
        }

        @Override
        public void checkChange( REC oldRecord, REC newRecord, CheckerEngine<REC, REP> engine,
                                 DiffRecordAccess records )
        {
            checker.checkChange( oldRecord, newRecord, engine, new ComparativeLogging( records, log ) );
        }

        @SuppressWarnings( "unchecked" )
        @Override
        public ComparativeRecordChecker<REC,PrimitiveRecord,REP> ownerCheck()
        {
            if ( checker instanceof OwningRecordCheck )
            {
                return ((OwningRecordCheck) checker).ownerCheck();
            }

            throw new UnsupportedOperationException();
        }
    }

    private static class LoggingReference<T extends AbstractBaseRecord> implements RecordReference<T>
    {
        private final RecordReference<T> reference;
        private final InvocationLog log;

        LoggingReference( RecordReference<T> reference, InvocationLog log )
        {
            this.reference = reference;
            this.log = log;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void dispatch( PendingReferenceCheck<T> reporter )
        {
            reference.dispatch( mock( (Class<PendingReferenceCheck<T>>) reporter.getClass(),
                    withSettings().spiedInstance( reporter )
                            .defaultAnswer( new ReporterSpy<>( reference, reporter, log ) ) ) );
        }
    }

    private static class ReporterSpy<T extends AbstractBaseRecord> implements Answer<Object>
    {
        private final RecordReference<T> reference;
        private final PendingReferenceCheck<T> reporter;
        private final InvocationLog log;

        public ReporterSpy( RecordReference<T> reference, PendingReferenceCheck<T> reporter, InvocationLog log )
        {
            this.reference = reference;
            this.reporter = reporter;
            this.log = log;
        }

        @Override
        public Object answer( InvocationOnMock invocation ) throws Throwable
        {
            if ( !(reference instanceof RecordReference.SkippingReference<?>) )
            {
                log.log( reporter, invocation );
            }
            return invocation.callRealMethod();
        }
    }

    private static class ComparativeLogging implements DiffRecordAccess
    {
        private final DiffRecordAccess access;
        private final InvocationLog log;

        ComparativeLogging( DiffRecordAccess access, InvocationLog log )
        {
            this.access = access;
            this.log = log;
        }

        private <T extends AbstractBaseRecord> LoggingReference<T> logging( RecordReference<T> actual )
        {
            return new LoggingReference<>( actual, log );
        }

        @Override
        public RecordReference<NodeRecord> previousNode( long id )
        {
            return logging( access.previousNode( id ) );
        }

        @Override
        public RecordReference<RelationshipRecord> previousRelationship( long id )
        {
            return logging( access.previousRelationship( id ) );
        }

        @Override
        public RecordReference<PropertyRecord> previousProperty( long id )
        {
            return logging( access.previousProperty( id ) );
        }

        @Override
        public RecordReference<NeoStoreRecord> previousGraph()
        {
            return logging( access.previousGraph() );
        }

        @Override
        public DynamicRecord changedSchema( long id )
        {
            return access.changedSchema( id );
        }

        @Override
        public NodeRecord changedNode( long id )
        {
            return access.changedNode( id );
        }

        @Override
        public RelationshipRecord changedRelationship( long id )
        {
            return access.changedRelationship( id );
        }

        @Override
        public RelationshipGroupRecord changedRelationshipGroup( long id )
        {
            return access.changedRelationshipGroup( id );
        }

        @Override
        public PropertyRecord changedProperty( long id )
        {
            return access.changedProperty( id );
        }

        @Override
        public DynamicRecord changedString( long id )
        {
            return access.changedString( id );
        }

        @Override
        public DynamicRecord changedArray( long id )
        {
            return access.changedArray( id );
        }

        @Override
        public RecordReference<DynamicRecord> schema( long id )
        {
            return logging( access.schema( id ) );
        }

        @Override
        public RecordReference<NodeRecord> node( long id )
        {
            return logging( access.node( id ) );
        }

        @Override
        public RecordReference<RelationshipRecord> relationship( long id )
        {
            return logging( access.relationship( id ) );
        }

        @Override
        public RecordReference<RelationshipGroupRecord> relationshipGroup( long id )
        {
            return logging( access.relationshipGroup( id ) );
        }

        @Override
        public RecordReference<PropertyRecord> property( long id )
        {
            return logging( access.property( id ) );
        }

        @Override
        public RecordReference<RelationshipTypeTokenRecord> relationshipType( int id )
        {
            return logging( access.relationshipType( id ) );
        }

        @Override
        public RecordReference<PropertyKeyTokenRecord> propertyKey( int id )
        {
            return logging( access.propertyKey( id ) );
        }

        @Override
        public RecordReference<DynamicRecord> string( long id )
        {
            return logging( access.string( id ) );
        }

        @Override
        public RecordReference<DynamicRecord> array( long id )
        {
            return logging( access.array( id ) );
        }

        @Override
        public RecordReference<DynamicRecord> relationshipTypeName( int id )
        {
            return logging( access.relationshipTypeName( id ) );
        }

        @Override
        public RecordReference<DynamicRecord> nodeLabels( long id )
        {
            return logging( access.nodeLabels( id ) );
        }

        @Override
        public RecordReference<LabelTokenRecord> label( int id )
        {
            return logging( access.label( id ) );
        }

        @Override
        public RecordReference<DynamicRecord> labelName( int id )
        {
            return logging( access.labelName( id ) );
        }

        @Override
        public RecordReference<DynamicRecord> propertyKeyName( int id )
        {
            return logging( access.propertyKeyName( id ) );
        }

        @Override
        public RecordReference<NeoStoreRecord> graph()
        {
            return logging( access.graph() );
        }
    }
}
