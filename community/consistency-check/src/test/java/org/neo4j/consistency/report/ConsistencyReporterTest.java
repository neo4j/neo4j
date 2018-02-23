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
package org.neo4j.consistency.report;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.consistency.store.synthetic.CountsEntry;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static java.lang.String.format;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;

@RunWith( Suite.class )
@Suite.SuiteClasses( {ConsistencyReporterTest.TestAllReportMessages.class,
        ConsistencyReporterTest.TestReportLifecycle.class} )
public class ConsistencyReporterTest
{
    public static class TestReportLifecycle
    {
        @Rule
        public final TestName testName = new TestName();

        @Test
        public void shouldSummarizeStatisticsAfterCheck()
        {
            // given
            ConsistencySummaryStatistics summary = mock( ConsistencySummaryStatistics.class );
            @SuppressWarnings( "unchecked" )
            RecordAccess records = mock( RecordAccess.class );
            ConsistencyReporter.ReportHandler handler = new ConsistencyReporter.ReportHandler(
                    new InconsistencyReport( mock( InconsistencyLogger.class ), summary ),
                    mock( ConsistencyReporter.ProxyFactory.class ), RecordType.PROPERTY, records,
                    new PropertyRecord( 0 ), NO_MONITOR );

            // when
            handler.updateSummary();

            // then
            verify( summary ).update( RecordType.PROPERTY, 0, 0 );
            verifyNoMoreInteractions( summary );
        }

        @Test
        @SuppressWarnings( "unchecked" )
        public void shouldOnlySummarizeStatisticsWhenAllReferencesAreChecked()
        {
            // given
            ConsistencySummaryStatistics summary = mock( ConsistencySummaryStatistics.class );
            RecordAccess records = mock( RecordAccess.class );
            ConsistencyReporter.ReportHandler handler = new ConsistencyReporter.ReportHandler(
                    new InconsistencyReport( mock( InconsistencyLogger.class ), summary ),
                    mock( ConsistencyReporter.ProxyFactory.class ), RecordType.PROPERTY, records,
                    new PropertyRecord( 0 ), NO_MONITOR );

            RecordReference<PropertyRecord> reference = mock( RecordReference.class );
            ComparativeRecordChecker<PropertyRecord, PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
                    checker = mock( ComparativeRecordChecker.class );

            handler.comparativeCheck( reference, checker );
            ArgumentCaptor<PendingReferenceCheck<PropertyRecord>> captor =
                    (ArgumentCaptor) ArgumentCaptor.forClass( PendingReferenceCheck.class );
            verify( reference ).dispatch( captor.capture() );
            PendingReferenceCheck pendingRefCheck = captor.getValue();

            // when
            handler.updateSummary();

            // then
            verifyZeroInteractions( summary );

            // when
            pendingRefCheck.skip();

            // then
            verify( summary ).update( RecordType.PROPERTY, 0, 0 );
            verifyNoMoreInteractions( summary );
        }

        @Test
        public void shouldIncludeStackTraceInUnexpectedCheckException()
        {
            // GIVEN
            ConsistencySummaryStatistics summary = mock( ConsistencySummaryStatistics.class );
            RecordAccess records = mock( RecordAccess.class );
            final AtomicReference<String> loggedError = new AtomicReference<>();
            InconsistencyLogger logger = new InconsistencyLogger()
            {
                @Override
                public void error( RecordType recordType, AbstractBaseRecord record, String message, Object[] args )
                {
                    assertTrue( loggedError.compareAndSet( null, message ) );
                }

                @Override
                public void error( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                        String message, Object[] args )
                {
                    assertTrue( loggedError.compareAndSet( null, message ) );
                }

                @Override
                public void warning( RecordType recordType, AbstractBaseRecord record, String message, Object[] args )
                {
                }

                @Override
                public void warning( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                        String message, Object[] args )
                {
                }
            };
            InconsistencyReport inconsistencyReport = new InconsistencyReport( logger, summary );
            ConsistencyReporter reporter = new ConsistencyReporter( records, inconsistencyReport );
            NodeRecord node = new NodeRecord( 10 );
            RecordCheck<NodeRecord,NodeConsistencyReport> checker = mock( RecordCheck.class );
            RuntimeException exception = new RuntimeException( "My specific exception" );
            doThrow( exception ).when( checker )
                    .check( any( NodeRecord.class ), any( CheckerEngine.class ), any( RecordAccess.class ) );

            // WHEN
            reporter.forNode( node, checker );

            // THEN
            assertNotNull( loggedError.get() );
            String error = loggedError.get();
            assertThat( error, containsString( "at " ) );
            assertThat( error, containsString( testName.getMethodName() ) );
        }
    }

    @RunWith( Parameterized.class )
    public static class TestAllReportMessages implements Answer
    {
        @Test
        @SuppressWarnings( "unchecked" )
        public void shouldLogInconsistency() throws Exception
        {
            // given
            InconsistencyReport report = mock( InconsistencyReport.class );
            ConsistencyReport.Reporter reporter = new ConsistencyReporter(
                    mock( RecordAccess.class ), report );

            // when
            reportMethod.invoke( reporter, parameters( reportMethod ) );

            // then
            if ( method.getAnnotation( ConsistencyReport.Warning.class ) == null )
            {
                if ( reportMethod.getName().endsWith( "Change" ) )
                {
                    verify( report ).error( any( RecordType.class ),
                            any( AbstractBaseRecord.class ), any( AbstractBaseRecord.class ),
                            argThat( hasExpectedFormat() ), any( Object[].class ) );
                }
                else
                {
                    verify( report ).error( any( RecordType.class ),
                            any( AbstractBaseRecord.class ), argThat( hasExpectedFormat() ), nullSafeAny() );
                }
            }
            else
            {
                if ( reportMethod.getName().endsWith( "Change" ) )
                {
                    verify( report ).warning( any( RecordType.class ),
                            any( AbstractBaseRecord.class ), any( AbstractBaseRecord.class ),
                            argThat( hasExpectedFormat() ), any( Object[].class ) );
                }
                else
                {
                    verify( report ).warning( any( RecordType.class ),
                            any( AbstractBaseRecord.class ),
                            argThat( hasExpectedFormat() ), nullSafeAny() );
                }
            }
        }

        private final Method reportMethod;
        private final Method method;

        public TestAllReportMessages( Method reportMethod, Method method )
        {
            this.reportMethod = reportMethod;
            this.method = method;
        }

        @Parameterized.Parameters( name = "{1}" )
        public static List<Object[]> methods()
        {
            ArrayList<Object[]> methods = new ArrayList<>();
            for ( Method reporterMethod : ConsistencyReport.Reporter.class.getMethods() )
            {
                Type[] parameterTypes = reporterMethod.getGenericParameterTypes();
                ParameterizedType checkerParameter = (ParameterizedType) parameterTypes[parameterTypes.length - 1];
                Class reportType = (Class) checkerParameter.getActualTypeArguments()[1];
                for ( Method method : reportType.getMethods() )
                {
                    methods.add( new Object[]{reporterMethod, method} );
                }
            }
            return methods;
        }

        @Rule
        public final TestRule logFailure = ( base, description ) -> new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    base.evaluate();
                }
                catch ( Throwable failure )
                {
                    System.err.println( "Failure in " + TestAllReportMessages.this + ": " + failure );
                    throw failure;
                }
            }
        };

        @Override
        public String toString()
        {
            return format( "report.%s( %s{ reporter.%s(); } )",
                    reportMethod.getName(), signatureOf( reportMethod ), method.getName() );
        }

        private static String signatureOf( Method reportMethod )
        {
            if ( reportMethod.getParameterTypes().length == 2 )
            {
                return "record, RecordCheck( reporter )";
            }
            else
            {
                return "oldRecord, newRecord, RecordCheck( reporter )";
            }
        }

        private Object[] parameters( Method method )
        {
            Class<?>[] parameterTypes = method.getParameterTypes();
            Object[] parameters = new Object[parameterTypes.length];
            for ( int i = 0; i < parameters.length; i++ )
            {
                parameters[i] = parameter( parameterTypes[i] );
            }
            return parameters;
        }

        private Object parameter( Class<?> type )
        {
            if ( type == RecordType.class )
            {
                return RecordType.STRING_PROPERTY;
            }
            if ( type == RecordCheck.class )
            {
                return mockChecker();
            }
            if ( type == NodeRecord.class )
            {
                return new NodeRecord( 0, false, 1, 2 );
            }
            if ( type == RelationshipRecord.class )
            {
                return new RelationshipRecord( 0, 1, 2, 3 );
            }
            if ( type == PropertyRecord.class )
            {
                return new PropertyRecord( 0 );
            }
            if ( type == PropertyKeyTokenRecord.class )
            {
                return new PropertyKeyTokenRecord( 0 );
            }
            if ( type == PropertyBlock.class )
            {
                return new PropertyBlock();
            }
            if ( type == RelationshipTypeTokenRecord.class )
            {
                return new RelationshipTypeTokenRecord( 0 );
            }
            if ( type == LabelTokenRecord.class )
            {
                return new LabelTokenRecord( 0 );
            }
            if ( type == DynamicRecord.class )
            {
                return new DynamicRecord( 0 );
            }
            if ( type == NeoStoreRecord.class )
            {
                return new NeoStoreRecord();
            }
            if ( type == LabelScanDocument.class )
            {
                return new LabelScanDocument( new NodeLabelRange( 0, new long[][] {} ) );
            }
            if ( type == IndexEntry.class )
            {
                return new IndexEntry( 0 );
            }
            if ( type == CountsEntry.class )
            {
                return new CountsEntry( nodeKey( 7 ), 42 );
            }
            if ( type == SchemaRule.Kind.class )
            {
                return SchemaRule.Kind.INDEX_RULE;
            }
            if ( type == IndexRule.class )
            {
                return IndexRule.indexRule( 1, IndexDescriptorFactory.forLabel( 2, 3 ),
                        new SchemaIndexProvider.Descriptor( "provider", "version" ) );
            }
            if ( type == SchemaRule.class )
            {
                return simpleSchemaRule();
            }
            if ( type == RelationshipGroupRecord.class )
            {
                return new RelationshipGroupRecord( 0, 1 );
            }
            if ( type == long.class )
            {
                return 12L;
            }
            if ( type == Object.class )
            {
                return "object";
            }
            throw new IllegalArgumentException( format( "Don't know how to provide parameter of type %s", type.getName() ) );
        }

        private SchemaRule simpleSchemaRule()
        {
            return new SchemaRule( 0 )
            {
                @Override
                public byte[] serialize()
                {
                    return new byte[0];
                }

                @Override
                public SchemaDescriptor schema()
                {
                    return null;
                }
            };
        }

        @SuppressWarnings( "unchecked" )
        private RecordCheck mockChecker()
        {
            RecordCheck checker = mock( RecordCheck.class );
            doAnswer( this ).when( checker ).check( any( AbstractBaseRecord.class ),
                    any( CheckerEngine.class ),
                    any( RecordAccess.class ) );
            return checker;
        }

        @Override
        public Object answer( InvocationOnMock invocation ) throws Throwable
        {
            Object[] arguments = invocation.getArguments();
            ConsistencyReport report = ((CheckerEngine)arguments[arguments.length - 2]).report();
            try
            {
                return method.invoke( report, parameters( method ) );
            }
            catch ( IllegalArgumentException ex )
            {
                throw new IllegalArgumentException(
                        format( "%s.%s#%s(...)", report, method.getDeclaringClass().getSimpleName(), method.getName() ),
                        ex );
            }
        }
    }

    private static <T> T[] nullSafeAny()
    {
        return ArgumentMatchers.argThat( argument -> true );
    }

    private static Matcher<String> hasExpectedFormat()
    {
        return new TypeSafeMatcher<String>()
        {
            @Override
            public boolean matchesSafely( String item )
            {
                return item.trim().split( " " ).length > 1;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "message of valid format" );
            }
        };
    }
}
