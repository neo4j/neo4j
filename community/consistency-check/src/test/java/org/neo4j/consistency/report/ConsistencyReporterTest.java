/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.consistency.report;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.annotations.documented.Warning;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.consistency.store.synthetic.CountsEntry;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.TokenScanDocument;
import org.neo4j.internal.index.label.EntityTokenRange;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.test.InMemoryTokens;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyVararg;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.consistency.report.ConsistencyReporter.NO_MONITOR;
import static org.neo4j.internal.counts.CountsKey.nodeKey;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

class ConsistencyReporterTest
{
    @Nested
    class TestReportLifecycle
    {
        @Test
        void shouldSummarizeStatisticsAfterCheck()
        {
            // given
            ConsistencySummaryStatistics summary = mock( ConsistencySummaryStatistics.class );
            RecordAccess records = mock( RecordAccess.class );
            ConsistencyReporter.ReportHandler handler = new ConsistencyReporter.ReportHandler(
                    new InconsistencyReport( mock( InconsistencyLogger.class ), summary ),
                    mock( ConsistencyReporter.ProxyFactory.class ), RecordType.PROPERTY, records,
                    new PropertyRecord( 0 ), NO_MONITOR, PageCacheTracer.NULL );

            // then
            verifyNoMoreInteractions( summary );
        }

        @Test
        @SuppressWarnings( "unchecked" )
        void shouldOnlySummarizeStatisticsWhenAllReferencesAreChecked()
        {
            // given
            ConsistencySummaryStatistics summary = mock( ConsistencySummaryStatistics.class );
            RecordAccess records = mock( RecordAccess.class );
            ConsistencyReporter.ReportHandler handler = new ConsistencyReporter.ReportHandler(
                    new InconsistencyReport( mock( InconsistencyLogger.class ), summary ),
                    mock( ConsistencyReporter.ProxyFactory.class ), RecordType.PROPERTY, records,
                    new PropertyRecord( 0 ), NO_MONITOR, PageCacheTracer.NULL );

            RecordReference<PropertyRecord> reference = mock( RecordReference.class );
            ComparativeRecordChecker<PropertyRecord, PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
                    checker = mock( ComparativeRecordChecker.class );

            handler.comparativeCheck( reference, checker );
            ArgumentCaptor<PendingReferenceCheck<PropertyRecord>> captor =
                    (ArgumentCaptor) ArgumentCaptor.forClass( PendingReferenceCheck.class );
            verify( reference ).dispatch( captor.capture() );
            PendingReferenceCheck pendingRefCheck = captor.getValue();

            // then
            verifyNoInteractions( summary );

            // when
            pendingRefCheck.skip();

            // then
            verifyNoMoreInteractions( summary );
        }

        @Test
        void shouldIncludeStackTraceInUnexpectedCheckException( TestInfo testInfo )
        {
            // GIVEN
            ConsistencySummaryStatistics summary = mock( ConsistencySummaryStatistics.class );
            RecordAccess records = mock( RecordAccess.class );
            final AtomicReference<String> loggedError = new AtomicReference<>();
            InconsistencyLogger logger = new InconsistencyLogger()
            {
                @Override
                public void error( RecordType recordType, AbstractBaseRecord record, String message, Object... args )
                {
                    assertTrue( loggedError.compareAndSet( null, message ) );
                }

                @Override
                public void error( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                        String message, Object... args )
                {
                    assertTrue( loggedError.compareAndSet( null, message ) );
                }

                @Override
                public void error( String message )
                {
                    assertTrue( loggedError.compareAndSet( null, message ) );
                }

                @Override
                public void warning( RecordType recordType, AbstractBaseRecord record, String message, Object... args )
                {
                }

                @Override
                public void warning( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                        String message, Object... args )
                {
                }

                @Override
                public void warning( String message )
                {
                }
            };
            InconsistencyReport inconsistencyReport = new InconsistencyReport( logger, summary );
            ConsistencyReporter reporter = new ConsistencyReporter( records, inconsistencyReport, PageCacheTracer.NULL );
            NodeRecord node = new NodeRecord( 10 );
            RecordCheck<NodeRecord,NodeConsistencyReport> checker = mock( RecordCheck.class );
            RuntimeException exception = new RuntimeException( "My specific exception" );
            doThrow( exception ).when( checker )
                    .check( any( NodeRecord.class ), any( CheckerEngine.class ), any( RecordAccess.class ), any( PageCursorTracer.class ) );

            // WHEN
            reporter.forNode( node, checker, NULL );

            // THEN
            String error = loggedError.get();
            assertThat( error ).contains( "at " );
            assertThat( error ).contains( testInfo.getTestMethod().orElseThrow().getName() );
        }
    }

    @Nested
    class TestAllReportMessages
    {
        @ParameterizedTest
        @MethodSource( value = "org.neo4j.consistency.report.ConsistencyReporterTest#methods" )
        void shouldLogInconsistency( ReportMethods methods ) throws Exception
        {
            // given
            Method reportMethod = methods.reportedMethod;
            Method method = methods.method;
            InconsistencyReport report = mock( InconsistencyReport.class );
            ConsistencyReport.Reporter reporter = new ConsistencyReporter(
                    mock( RecordAccess.class ), report, PageCacheTracer.NULL );

            // when
            reportMethod.invoke( reporter, parameters( reportMethod ) );

            // then
            if ( method.getAnnotation( Warning.class ) == null )
            {
                if ( reportMethod.getName().endsWith( "Change" ) )
                {
                    verify( report ).error( any( RecordType.class ),
                                            any( AbstractBaseRecord.class ), any( AbstractBaseRecord.class ),
                                            argThat( expectedFormat() ), any( Object[].class ) );
                }
                else
                {
                    verify( report ).error( any( RecordType.class ),
                                            any( AbstractBaseRecord.class ), argThat( expectedFormat() ), anyVararg() );
                }
            }
            else
            {
                if ( reportMethod.getName().endsWith( "Change" ) )
                {
                    verify( report ).warning( any( RecordType.class ),
                                              any( AbstractBaseRecord.class ), any( AbstractBaseRecord.class ),
                                              argThat( expectedFormat() ), any( Object[].class ) );
                }
                else
                {
                    verify( report ).error( any( RecordType.class ),
                                              any( AbstractBaseRecord.class ),
                                              argThat( expectedFormat() ), anyVararg() );
                }
            }
        }

        private Object[] parameters( Method method )
        {
            Class<?>[] parameterTypes = method.getParameterTypes();
            Object[] parameters = new Object[parameterTypes.length];
            for ( int i = 0; i < parameters.length; i++ )
            {
                parameters[i] = parameter( parameterTypes[i], method );
            }
            return parameters;
        }

        private Object parameter( Class<?> type, Method method )
        {
            if ( type == RecordType.class )
            {
                return RecordType.STRING_PROPERTY;
            }
            if ( type == RecordCheck.class )
            {
                return mockChecker( method );
            }
            if ( type == NodeRecord.class )
            {
                return new NodeRecord( 0 ).initialize( false, 2, false, 1, 0 );
            }
            if ( type == RelationshipRecord.class )
            {
                RelationshipRecord relationship = new RelationshipRecord( 0 );
                relationship.setLinks( 1, 2, 3 );
                return relationship;
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
            if ( type == TokenScanDocument.class )
            {
                return new TokenScanDocument( new EntityTokenRange( 0, new long[][] {}, NODE ) );
            }
            if ( type == IndexEntry.class )
            {
                return new IndexEntry( IndexPrototype.forSchema( SchemaDescriptor.forLabel( 1, 1 ) )
                        .withName( "index" ).materialise( 1L ), new InMemoryTokens(), 0 );
            }
            if ( type == CountsEntry.class )
            {
                return new CountsEntry( nodeKey( 7 ), 42 );
            }
            if ( type == IndexDescriptor.class )
            {
                return IndexPrototype.forSchema( forLabel( 2, 3 ), IndexProviderDescriptor.UNDECIDED )
                        .withName( "index" ).materialise( 1 );
            }
            if ( type == SchemaRule.class )
            {
                return simpleSchemaRule();
            }
            if ( type == SchemaRecord.class )
            {
                return new SchemaRecord( 42 );
            }
            if ( type == RelationshipGroupRecord.class )
            {
                return new RelationshipGroupRecord( 0 )
                        .initialize( false, 1, NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue(),
                                NULL_REFERENCE.longValue() );
            }
            if ( type == long.class )
            {
                return 12L;
            }
            if ( type == PageCursorTracer.class )
            {
                return NULL;
            }
            if ( type == Object.class )
            {
                return "object";
            }
            throw new IllegalArgumentException( format( "Don't know how to provide parameter of type %s", type.getName() ) );
        }

        private SchemaRule simpleSchemaRule()
        {
            return new SchemaRule()
            {
                @Override
                public long getId()
                {
                    return 0;
                }

                @Override
                public String getName()
                {
                    return null;
                }

                @Override
                public SchemaRule withName( String name )
                {
                    return null;
                }

                @Override
                public SchemaDescriptor schema()
                {
                    return null;
                }

                @Override
                public String userDescription( TokenNameLookup tokenNameLookup )
                {
                    return null;
                }
            };
        }

        @SuppressWarnings( "unchecked" )
        private RecordCheck mockChecker( Method method )
        {
            RecordCheck checker = mock( RecordCheck.class );
            doAnswer( invocation ->
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
            } ).when( checker ).check( any( AbstractBaseRecord.class ),
                                                    any( CheckerEngine.class ),
                                                    any( RecordAccess.class ), any( PageCursorTracer.class ) );
            return checker;
        }
    }

    private static ArgumentMatcher<String> expectedFormat()
    {
        return argument -> argument.trim().split( " " ).length > 1;
    }

    public static List<ReportMethods> methods()
    {
        List<ReportMethods> methods = new ArrayList<>();
        for ( Method reporterMethod : ConsistencyReport.Reporter.class.getMethods() )
        {
            if ( reporterMethod.getReturnType() == Void.TYPE )
            {
                Type[] parameterTypes = reporterMethod.getGenericParameterTypes();
                ParameterizedType checkerParameter = findParametrizedType( parameterTypes );
                Class reportType = (Class) checkerParameter.getActualTypeArguments()[1];
                for ( Method method : reportType.getMethods() )
                {
                    methods.add( new ReportMethods( reporterMethod, method ) );
                }
            }
        }
        return methods;
    }

    private static ParameterizedType findParametrizedType( Type[] types )
    {
        for ( Type type : types )
        {
            if ( type instanceof ParameterizedType )
            {
                return (ParameterizedType) type;
            }
        }
        throw new IllegalStateException( "Parametrized type expected but not found. Actual types: " + Arrays.toString( types ) );
    }

    public static class ReportMethods
    {
        final Method reportedMethod;
        final Method method;

        ReportMethods( Method reportedMethod, Method method )
        {
            this.reportedMethod = reportedMethod;
            this.method = method;
        }
    }

    private static <T> T[] nullSafeAny()
    {
        return argThat( argument -> true );
    }
}
