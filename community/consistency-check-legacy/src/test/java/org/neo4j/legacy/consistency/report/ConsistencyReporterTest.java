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
package org.neo4j.legacy.consistency.report;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.api.impl.index.LuceneNodeLabelRange;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
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
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.legacy.consistency.RecordType;
import org.neo4j.legacy.consistency.checking.CheckerEngine;
import org.neo4j.legacy.consistency.checking.ComparativeRecordChecker;
import org.neo4j.legacy.consistency.checking.RecordCheck;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReporter;
import org.neo4j.legacy.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.legacy.consistency.report.InconsistencyLogger;
import org.neo4j.legacy.consistency.report.InconsistencyReport;
import org.neo4j.legacy.consistency.report.PendingReferenceCheck;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;
import org.neo4j.legacy.consistency.store.RecordReference;
import org.neo4j.legacy.consistency.store.synthetic.CountsEntry;
import org.neo4j.legacy.consistency.store.synthetic.IndexEntry;
import org.neo4j.legacy.consistency.store.synthetic.LabelScanDocument;

import static java.lang.String.format;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;

@RunWith(Suite.class)
@Suite.SuiteClasses({ConsistencyReporterTest.TestAllReportMessages.class,
                     ConsistencyReporterTest.TestReportLifecycle.class})
public class ConsistencyReporterTest
{
    public static class TestReportLifecycle
    {
        @Test
        public void shouldSummarizeStatisticsAfterCheck()
        {
            // given
            ConsistencySummaryStatistics summary = mock( ConsistencySummaryStatistics.class );
            @SuppressWarnings("unchecked")
            ConsistencyReporter.ReportHandler handler = new ConsistencyReporter.ReportHandler(
                    new InconsistencyReport( mock( InconsistencyLogger.class ), summary ),
                    mock( ConsistencyReporter.ProxyFactory.class ), RecordType.PROPERTY, new PropertyRecord( 0 ) );

            // when
            handler.updateSummary();

            // then
            verify( summary ).update( RecordType.PROPERTY, 0, 0 );
            verifyNoMoreInteractions( summary );
        }

        @Test
        @SuppressWarnings("unchecked")
        public void shouldOnlySummarizeStatisticsWhenAllReferencesAreChecked()
        {
            // given
            ConsistencySummaryStatistics summary = mock( ConsistencySummaryStatistics.class );
            ConsistencyReporter.ReportHandler handler = new ConsistencyReporter.ReportHandler(
                    new InconsistencyReport( mock( InconsistencyLogger.class ), summary ),
                    mock( ConsistencyReporter.ProxyFactory.class ), RecordType.PROPERTY, new PropertyRecord( 0 ) );

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
    }

    @RunWith(Parameterized.class)
    public static class TestAllReportMessages implements Answer
    {
        @Test
        @SuppressWarnings("unchecked")
        public void shouldLogInconsistency() throws Exception
        {
            // given
            InconsistencyReport report = mock( InconsistencyReport.class );
            ConsistencyReport.Reporter reporter = new ConsistencyReporter(
                    mock( DiffRecordAccess.class ), report );

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
                                            any( AbstractBaseRecord.class ),
                                            argThat( hasExpectedFormat() ), any( Object[].class ) );
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
                                              argThat( hasExpectedFormat() ), any( Object[].class ) );
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

        @Parameterized.Parameters(name="{1}")
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
        public final TestRule logFailure = new TestRule()
        {
            @Override
            public Statement apply( final Statement base, org.junit.runner.Description description )
            {
                return new Statement()
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
                return new LabelScanDocument( new LuceneNodeLabelRange( 0, new long[] {}, new long[][] {} ) );
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
                return IndexRule.indexRule( 1, 2, 3, new SchemaIndexProvider.Descriptor( "provider", "version" ) );
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

        @SuppressWarnings("unchecked")
        private RecordCheck mockChecker()
        {
            RecordCheck checker = mock( RecordCheck.class );
            doAnswer( this ).when( checker ).check( any( AbstractBaseRecord.class ),
                                                    any( CheckerEngine.class ),
                                                    any( RecordAccess.class ) );
            doAnswer( this ).when( checker ).checkChange( any( AbstractBaseRecord.class ),
                                                          any( AbstractBaseRecord.class ),
                                                          any( CheckerEngine.class ),
                                                          any( DiffRecordAccess.class ) );
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
