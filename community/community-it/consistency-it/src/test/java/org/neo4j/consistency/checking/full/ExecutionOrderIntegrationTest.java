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
package org.neo4j.consistency.checking.full;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.GraphStoreFixture;
import org.neo4j.consistency.checking.OwningRecordCheck;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.DefaultCacheAccess;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NeoStoreConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyKeyTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipTypeConsistencyReport;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.consistency.report.InconsistencyLogger;
import org.neo4j.consistency.report.InconsistencyReport;
import org.neo4j.consistency.report.PendingReferenceCheck;
import org.neo4j.consistency.statistics.DefaultCounts;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.withSettings;
import static org.neo4j.consistency.ConsistencyCheckService.defaultConsistencyCheckThreadsNumber;
import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

public class ExecutionOrderIntegrationTest
{
    @Rule
    public final GraphStoreFixture fixture = new GraphStoreFixture( getRecordFormatName() )
    {
        @Override
        protected void generateInitialData( GraphDatabaseService graphDb )
        {
            // TODO: create bigger sample graph here
            try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
            {
                Node node1 = set( graphDb.createNode( label( "Foo" ) ) );
                Node node2 = set( graphDb.createNode( label( "Foo" ) ), property( "key", "value" ) );
                node1.createRelationshipTo( node2, RelationshipType.withName( "C" ) );
                tx.success();
            }
        }
    };

    @Test
    public void shouldRunChecksInSingleThreadedPass() throws Exception
    {
        // given
        StoreAccess store = fixture.directStoreAccess().nativeStores();
        int threads = defaultConsistencyCheckThreadsNumber();
        CacheAccess cacheAccess = new DefaultCacheAccess( new DefaultCounts( threads ), threads );
        RecordAccess access = FullCheck.recordAccess( store, cacheAccess );

        FullCheck singlePass = new FullCheck( getTuningConfiguration(), ProgressMonitorFactory.NONE, Statistics.NONE, threads );

        ConsistencySummaryStatistics singlePassSummary = new ConsistencySummaryStatistics();
        InconsistencyLogger logger = mock( InconsistencyLogger.class );
        InvocationLog singlePassChecks = new InvocationLog();

        // when
        singlePass.execute( fixture.directStoreAccess(), new LogDecorator( singlePassChecks ), access, new InconsistencyReport( logger,
                singlePassSummary ), cacheAccess, NO_MONITOR );

        // then
        verifyZeroInteractions( logger );
        assertEquals( "Expected no inconsistencies in single pass.",
                0, singlePassSummary.getTotalInconsistencyCount() );
    }

    private Config getTuningConfiguration()
    {
        return Config.defaults( stringMap( GraphDatabaseSettings.pagecache_memory.name(), "8m",
                GraphDatabaseSettings.record_format.name(), getRecordFormatName() ) );
    }

    protected String getRecordFormatName()
    {
        return StringUtils.EMPTY;
    }

    private static class InvocationLog
    {
        private final Map<String, Throwable> data = new HashMap<>();
        private final Map<String, Integer> duplicates = new HashMap<>();

        @SuppressWarnings( "ThrowableResultOfMethodCallIgnored" )
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
                            .append( '[' ).append( record.getId() ).append( ']' );
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

    private static class LogDecorator implements CheckDecorator
    {
        private final InvocationLog log;

        LogDecorator( InvocationLog log )
        {
            this.log = log;
        }

        @Override
        public void prepare()
        {
        }

        <REC extends AbstractBaseRecord, REP extends ConsistencyReport> OwningRecordCheck<REC, REP> logging(
                RecordCheck<REC, REP> checker )
        {
            return new LoggingChecker<>( checker, log );
        }

        @Override
        public OwningRecordCheck<NeoStoreRecord, NeoStoreConsistencyReport> decorateNeoStoreChecker(
                OwningRecordCheck<NeoStoreRecord, NeoStoreConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public OwningRecordCheck<NodeRecord, NodeConsistencyReport> decorateNodeChecker(
                OwningRecordCheck<NodeRecord, NodeConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public OwningRecordCheck<RelationshipRecord, RelationshipConsistencyReport> decorateRelationshipChecker(
                OwningRecordCheck<RelationshipRecord, RelationshipConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<PropertyRecord, PropertyConsistencyReport> decoratePropertyChecker(
                RecordCheck<PropertyRecord, PropertyConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> decoratePropertyKeyTokenChecker(
                RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport> decorateRelationshipTypeTokenChecker(
                RecordCheck<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport> checker )
        {
            return logging( checker );
        }

        @Override
        public RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> decorateLabelTokenChecker(
                RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> checker )
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
            checker.check( record, engine, new ComparativeLogging( records, log ) );
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

        @SuppressWarnings( "unchecked" )
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

        ReporterSpy( RecordReference<T> reference, PendingReferenceCheck<T> reporter, InvocationLog log )
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

    private static class ComparativeLogging implements RecordAccess
    {
        private final RecordAccess access;
        private final InvocationLog log;

        ComparativeLogging( RecordAccess access, InvocationLog log )
        {
            this.access = access;
            this.log = log;
        }

        private <T extends AbstractBaseRecord> LoggingReference<T> logging( RecordReference<T> actual )
        {
            return new LoggingReference<>( actual, log );
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

        @Override
        public boolean shouldCheck( long id, MultiPassStore store )
        {
            return access.shouldCheck( id, store );
        }

        @Override
        public CacheAccess cacheAccess()
        {
            return access.cacheAccess();
        }

        @Override
        public Iterator<PropertyRecord> rawPropertyChain( long firstId )
        {
            return access.rawPropertyChain( firstId );
        }
    }
}
