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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.annotations.documented.DocumentedUtils;
import org.neo4j.annotations.documented.Warning;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport.CountsConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicLabelConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.IndexConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelScanConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyKeyTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipTypeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipTypeScanConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.SchemaConsistencyReport;
import org.neo4j.consistency.store.DirectRecordReference;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.consistency.store.synthetic.CountsEntry;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.TokenScanDocument;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

import static java.util.Arrays.asList;
import static org.neo4j.consistency.report.ConsistencyReporter.ProxyFactory.create;
import static org.neo4j.internal.helpers.Exceptions.stringify;

public class ConsistencyReporter implements ConsistencyReport.Reporter
{
    private static final String CONSISTENCY_REPORT_READER_TAG = "consistencyReportReader";
    private static final ProxyFactory<SchemaConsistencyReport> SCHEMA_REPORT = create( SchemaConsistencyReport.class );
    private static final ProxyFactory<NodeConsistencyReport> NODE_REPORT = create( NodeConsistencyReport.class );
    private static final ProxyFactory<RelationshipConsistencyReport> RELATIONSHIP_REPORT = create( RelationshipConsistencyReport.class );
    private static final ProxyFactory<PropertyConsistencyReport> PROPERTY_REPORT = create( PropertyConsistencyReport.class );
    private static final ProxyFactory<RelationshipTypeConsistencyReport> RELATIONSHIP_TYPE_REPORT = create( RelationshipTypeConsistencyReport.class );
    private static final ProxyFactory<LabelTokenConsistencyReport> LABEL_KEY_REPORT = create( LabelTokenConsistencyReport.class );
    private static final ProxyFactory<PropertyKeyTokenConsistencyReport> PROPERTY_KEY_REPORT = create( PropertyKeyTokenConsistencyReport.class );
    private static final ProxyFactory<DynamicConsistencyReport> DYNAMIC_REPORT = create( DynamicConsistencyReport.class );
    private static final ProxyFactory<DynamicLabelConsistencyReport> DYNAMIC_LABEL_REPORT = create( DynamicLabelConsistencyReport.class );
    private static final ProxyFactory<LabelScanConsistencyReport> LABEL_SCAN_REPORT = create( LabelScanConsistencyReport.class );
    private static final ProxyFactory<RelationshipTypeScanConsistencyReport> RELATIONSHIP_TYPE_SCAN_REPORT =
            create( RelationshipTypeScanConsistencyReport.class );
    private static final ProxyFactory<IndexConsistencyReport> INDEX_REPORT = create( IndexConsistencyReport.class );
    private static final ProxyFactory<RelationshipGroupConsistencyReport> RELATIONSHIP_GROUP_REPORT = create( RelationshipGroupConsistencyReport.class );
    private static final ProxyFactory<CountsConsistencyReport> COUNTS_REPORT = create( CountsConsistencyReport.class );

    private final RecordAccess records;
    private final InconsistencyReport report;
    private final Monitor monitor;
    private final PageCacheTracer pageCacheTracer;

    public interface Monitor
    {
        void reported( Class<?> report, String method, String message );
    }

    public static final Monitor NO_MONITOR = ( report, method, message ) ->
    {
    };

    public ConsistencyReporter( RecordAccess records, InconsistencyReport report, PageCacheTracer pageCacheTracer )
    {
        this( records, report, NO_MONITOR, pageCacheTracer );
    }

    public ConsistencyReporter( RecordAccess records, InconsistencyReport report, Monitor monitor, PageCacheTracer pageCacheTracer )
    {
        this.records = records;
        this.report = report;
        this.monitor = monitor;
        this.pageCacheTracer = pageCacheTracer;
    }

    private <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport>
    void dispatch( RecordType type, ProxyFactory<REPORT> factory, RECORD record, RecordCheck<RECORD, REPORT> checker, PageCursorTracer cursorTracer )
    {
        ReportInvocationHandler<RECORD,REPORT> handler = new ReportHandler<>( report, factory, type, records, record, monitor, pageCacheTracer );
        try
        {
            checker.check( record, handler, records, cursorTracer );
        }
        catch ( Exception e )
        {
            // This is a rare event and exposing the stack trace is a good idea, otherwise we
            // can only see that something went wrong, not at all what.
            handler.report.error( type, record, "Failed to check record: " + stringify( e ),
                    new Object[0] );
        }
    }

    static void dispatchReference( CheckerEngine engine, ComparativeRecordChecker checker,
                                   AbstractBaseRecord referenced, RecordAccess records, PageCursorTracer cursorTracer )
    {
        ReportInvocationHandler handler = (ReportInvocationHandler) engine;
        handler.checkReference( engine, checker, referenced, records, cursorTracer );
    }

    static String pendingCheckToString( CheckerEngine engine, ComparativeRecordChecker checker )
    {
        ReportInvocationHandler handler = (ReportInvocationHandler) engine;
        return handler.pendingCheckToString(checker);
    }

    static void dispatchChangeReference( CheckerEngine engine, ComparativeRecordChecker checker,
                                         AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced,
                                         RecordAccess records, PageCursorTracer cursorTracer )
    {
        ReportInvocationHandler handler = (ReportInvocationHandler) engine;
        handler.checkDiffReference( engine, checker, oldReferenced, newReferenced, records, cursorTracer );
    }

    static void dispatchSkip( CheckerEngine engine )
    {
    }

    @Override
    public <RECORD extends AbstractBaseRecord,REPORT extends ConsistencyReport> REPORT report( RECORD record,
            Class<REPORT> cls, RecordType recordType )
    {
        ProxyFactory<REPORT> proxyFactory = ProxyFactory.get( cls );
        ReportInvocationHandler<RECORD,REPORT> handler = new ReportHandler<>( report, proxyFactory, recordType, records, record, monitor, pageCacheTracer )
        {
            @Override
            protected void inconsistencyReported()
            {
            }
        };
        return handler.report();
    }

    public static FormattingDocumentedHandler formattingHandler( InconsistencyReport report, RecordType type )
    {
        return new FormattingDocumentedHandler( report, type );
    }

    public static class FormattingDocumentedHandler implements InvocationHandler
    {
        private final InconsistencyReport report;
        private final RecordType type;
        private short errors;
        private short warnings;

        FormattingDocumentedHandler( InconsistencyReport report, RecordType type )
        {
            this.report = report;
            this.type = type;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args )
        {
            String message = DocumentedUtils.extractFormattedMessage( method, args );
            if ( method.getAnnotation( Warning.class ) == null )
            {
                errors++;
                report.error( message );
            }
            else
            {
                warnings++;
                report.warning( message );
            }
            return null;
        }

        public void updateSummary()
        {
            report.updateSummary( type, errors, warnings );
        }
    }

    public abstract static class ReportInvocationHandler
            <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport>
            implements CheckerEngine<RECORD, REPORT>, InvocationHandler
    {
        final InconsistencyReport report;
        private final ProxyFactory<REPORT> factory;
        final RecordType type;
        private short references = 1/*this*/;
        private final RecordAccess records;
        private final PageCacheTracer pageCacheTracer;
        private final Monitor monitor;

        private ReportInvocationHandler( InconsistencyReport report, ProxyFactory<REPORT> factory, RecordType type,
               RecordAccess records, Monitor monitor, PageCacheTracer pageCacheTracer )
        {
            this.report = report;
            this.factory = factory;
            this.type = type;
            this.records = records;
            this.monitor = monitor;
            this.pageCacheTracer = pageCacheTracer;
        }

        String pendingCheckToString( ComparativeRecordChecker checker )
        {
            String checkName;
            try
            {
                if ( checker.getClass().getMethod( "toString" ).getDeclaringClass() == Object.class )
                {
                    checkName = checker.getClass().getSimpleName();
                    if ( checkName.isEmpty() )
                    {
                        checkName = checker.getClass().getName();
                    }
                }
                else
                {
                    checkName = checker.toString();
                }
            }
            catch ( NoSuchMethodException e )
            {
                checkName = checker.toString();
            }
            return String.format( "ReferenceCheck{%s[%s]/%s}", type, recordId(), checkName );
        }

        abstract long recordId();

        @Override
        public <REFERRED extends AbstractBaseRecord> void comparativeCheck(
                RecordReference<REFERRED> reference, ComparativeRecordChecker<RECORD, ? super REFERRED, REPORT> checker )
        {
            references++;
            reference.dispatch( new PendingReferenceCheck<>( this, checker ) );
        }

        @Override
        public REPORT report()
        {
            return factory.create( this );
        }

        /**
         * Invoked when an inconsistency is encountered.
         *
         * @param args array of the items referenced from this record with which it is inconsistent.
         */
        @Override
        public Object invoke( Object proxy, Method method, Object[] args )
        {
            String message = DocumentedUtils.extractMessage( method );
            if ( method.getAnnotation( Warning.class ) == null )
            {
                args = getRealRecords( args );
                logError( message, args );
                report.updateSummary( type, 1, 0 );
            }
            else
            {
                args = getRealRecords( args );
                logWarning( message, args );
                report.updateSummary( type, 0, 1 );
            }
            monitor.reported( factory.type(), method.getName(), message );
            inconsistencyReported();
            return null;
        }

        protected void inconsistencyReported()
        {
        }

        private Object[] getRealRecords( Object[] args )
        {
            if ( args == null )
            {
                return args;
            }
            try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( CONSISTENCY_REPORT_READER_TAG ) )
            {
                for ( int i = 0; i < args.length; i++ )
                {
                    // We use "created" flag here. Consistency checking code revolves around records and so
                    // even in scenarios where records are built from other sources, f.ex half-and-purpose-built from cache,
                    // this flag is used to signal that the real record needs to be read in order to be used as a general
                    // purpose record.
                    if ( args[i] instanceof AbstractBaseRecord && ((AbstractBaseRecord) args[i]).isCreated() )
                    {   // get the real record
                        if ( args[i] instanceof NodeRecord )
                        {
                            args[i] = ((DirectRecordReference<NodeRecord>) records.node( ((NodeRecord) args[i]).getId(), cursorTracer )).record();
                        }
                        else if ( args[i] instanceof RelationshipRecord )
                        {
                            args[i] = ((DirectRecordReference<RelationshipRecord>) records.relationship(
                                    ((RelationshipRecord) args[i]).getId(), cursorTracer )).record();
                        }
                    }
                }
            }
            return args;
        }

        protected abstract void logError( String message, Object[] args );

        protected abstract void logWarning( String message, Object[] args );

        abstract void checkReference( CheckerEngine engine, ComparativeRecordChecker checker,
                                      AbstractBaseRecord referenced, RecordAccess records, PageCursorTracer cursorTracer );

        abstract void checkDiffReference( CheckerEngine engine, ComparativeRecordChecker checker,
                                          AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced,
                                          RecordAccess records, PageCursorTracer cursorTracer );
    }

    public static class ReportHandler
            <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport>
            extends ReportInvocationHandler<RECORD,REPORT>
    {
        private final AbstractBaseRecord record;

        public ReportHandler( InconsistencyReport report, ProxyFactory<REPORT> factory, RecordType type,
                RecordAccess records, AbstractBaseRecord record, Monitor monitor, PageCacheTracer pageCacheTracer )
        {
            super( report, factory, type, records, monitor, pageCacheTracer );
            this.record = record;
        }

        @Override
        long recordId()
        {
            return record.getId();
        }

        @Override
        protected void logError( String message, Object[] args )
        {
            report.error( type, record, message, args );
        }

        @Override
        protected void logWarning( String message, Object[] args )
        {
            report.warning( type, record, message, args );
        }

        @Override
        @SuppressWarnings( "unchecked" )
        void checkReference( CheckerEngine engine, ComparativeRecordChecker checker, AbstractBaseRecord referenced,
                             RecordAccess records, PageCursorTracer cursorTracer )
        {
            checker.checkReference( record, referenced, this, records, cursorTracer );
        }

        @Override
        @SuppressWarnings( "unchecked" )
        void checkDiffReference( CheckerEngine engine, ComparativeRecordChecker checker,
                                 AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced,
                                 RecordAccess records, PageCursorTracer cursorTracer )
        {
            checker.checkReference( record, newReferenced, this, records, cursorTracer );
        }
    }

    @Override
    public void forSchema( SchemaRecord schema,
                           RecordCheck<SchemaRecord, SchemaConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.SCHEMA, SCHEMA_REPORT, schema, checker, cursorTracer );
    }

    @Override
    public void forNode( NodeRecord node,
                         RecordCheck<NodeRecord, NodeConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.NODE, NODE_REPORT, node, checker, cursorTracer );
    }

    @Override
    public void forRelationship( RelationshipRecord relationship,
                                 RecordCheck<RelationshipRecord, RelationshipConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.RELATIONSHIP, RELATIONSHIP_REPORT, relationship, checker, cursorTracer );
    }

    @Override
    public void forProperty( PropertyRecord property,
                             RecordCheck<PropertyRecord, PropertyConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.PROPERTY, PROPERTY_REPORT, property, checker, cursorTracer );
    }

    @Override
    public void forRelationshipTypeName( RelationshipTypeTokenRecord relationshipTypeTokenRecord,
                                         RecordCheck<RelationshipTypeTokenRecord,
                                         RelationshipTypeConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.RELATIONSHIP_TYPE, RELATIONSHIP_TYPE_REPORT, relationshipTypeTokenRecord, checker, cursorTracer );
    }

    @Override
    public void forLabelName( LabelTokenRecord label,
                              RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.LABEL, LABEL_KEY_REPORT, label, checker, cursorTracer );
    }

    @Override
    public void forNodeLabelScan( TokenScanDocument document,
                                  RecordCheck<TokenScanDocument, LabelScanConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.LABEL_SCAN_DOCUMENT, LABEL_SCAN_REPORT, document, checker, cursorTracer );
    }

    @Override
    public void forRelationshipTypeScan( TokenScanDocument document,
            RecordCheck<TokenScanDocument,RelationshipTypeScanConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.RELATIONSHIP_TYPE_SCAN_DOCUMENT, RELATIONSHIP_TYPE_SCAN_REPORT, document, checker, cursorTracer );
    }

    @Override
    public void forIndexEntry( IndexEntry entry,
                               RecordCheck<IndexEntry, IndexConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.INDEX, INDEX_REPORT, entry, checker, cursorTracer );
    }

    @Override
    public void forPropertyKey( PropertyKeyTokenRecord key, RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> checker,
            PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.PROPERTY_KEY, PROPERTY_KEY_REPORT, key, checker, cursorTracer );
    }

    @Override
    public void forDynamicBlock( RecordType type, DynamicRecord record,
                                 RecordCheck<DynamicRecord, DynamicConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( type, DYNAMIC_REPORT, record, checker, cursorTracer );
    }

    @Override
    public void forDynamicLabelBlock( RecordType type, DynamicRecord record,
                                      RecordCheck<DynamicRecord, DynamicLabelConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( type, DYNAMIC_LABEL_REPORT, record, checker, cursorTracer );
    }

    @Override
    public void forRelationshipGroup( RelationshipGroupRecord record,
            RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.RELATIONSHIP_GROUP, RELATIONSHIP_GROUP_REPORT, record, checker, cursorTracer );
    }

    @Override
    public void forCounts( CountsEntry countsEntry,
                           RecordCheck<CountsEntry,CountsConsistencyReport> checker, PageCursorTracer cursorTracer )
    {
        dispatch( RecordType.COUNTS, COUNTS_REPORT, countsEntry, checker, cursorTracer );
    }

    // Plain and simple report instances

    @Override
    public SchemaConsistencyReport forSchema( SchemaRecord schema )
    {
        return report( SCHEMA_REPORT, RecordType.SCHEMA, schema );
    }

    @Override
    public NodeConsistencyReport forNode( NodeRecord node )
    {
        return report( NODE_REPORT, RecordType.NODE, node );
    }

    @Override
    public RelationshipConsistencyReport forRelationship( RelationshipRecord relationship )
    {
        return report( RELATIONSHIP_REPORT, RecordType.RELATIONSHIP, relationship );
    }

    @Override
    public PropertyConsistencyReport forProperty( PropertyRecord property )
    {
        return report( PROPERTY_REPORT, RecordType.PROPERTY, property );
    }

    @Override
    public RelationshipTypeConsistencyReport forRelationshipTypeName( RelationshipTypeTokenRecord relationshipType )
    {
        return report( RELATIONSHIP_TYPE_REPORT, RecordType.RELATIONSHIP_TYPE, relationshipType );
    }

    @Override
    public LabelTokenConsistencyReport forLabelName( LabelTokenRecord label )
    {
        return report( LABEL_KEY_REPORT, RecordType.LABEL, label );
    }

    @Override
    public PropertyKeyTokenConsistencyReport forPropertyKey( PropertyKeyTokenRecord key )
    {
        return report( PROPERTY_KEY_REPORT, RecordType.PROPERTY_KEY, key );
    }

    @Override
    public DynamicConsistencyReport forDynamicBlock( RecordType type, DynamicRecord record )
    {
        return report( DYNAMIC_REPORT, type, record );
    }

    @Override
    public DynamicLabelConsistencyReport forDynamicLabelBlock( RecordType type, DynamicRecord record )
    {
        return report( DYNAMIC_LABEL_REPORT, type, record );
    }

    @Override
    public LabelScanConsistencyReport forNodeLabelScan( TokenScanDocument document )
    {
        return report( LABEL_SCAN_REPORT, RecordType.LABEL_SCAN_DOCUMENT, document );
    }

    @Override
    public RelationshipTypeScanConsistencyReport forRelationshipTypeScan( TokenScanDocument document )
    {
        return report( RELATIONSHIP_TYPE_SCAN_REPORT, RecordType.RELATIONSHIP_TYPE_SCAN_DOCUMENT, document );
    }

    @Override
    public IndexConsistencyReport forIndexEntry( IndexEntry entry )
    {
        return report( INDEX_REPORT, RecordType.INDEX, entry );
    }

    @Override
    public RelationshipGroupConsistencyReport forRelationshipGroup( RelationshipGroupRecord group )
    {
        return report( RELATIONSHIP_GROUP_REPORT, RecordType.RELATIONSHIP_GROUP, group );
    }

    @Override
    public CountsConsistencyReport forCounts( CountsEntry countsEntry )
    {
        return report( COUNTS_REPORT, RecordType.COUNTS, countsEntry );
    }

    private <RECORD extends AbstractBaseRecord,REPORT extends ConsistencyReport> REPORT report( ProxyFactory<REPORT> factory, RecordType type, RECORD record )
    {
        return new ReportHandler<>( report, factory, type, records, record, monitor, pageCacheTracer ).report();
    }

    public static class ProxyFactory<T>
    {
        private static final Map<Class<?>,ProxyFactory<?>> INSTANCES = new HashMap<>();
        private Constructor<? extends T> constructor;
        private final Class<T> type;

        @SuppressWarnings( "unchecked" )
        static <T> ProxyFactory<T> get( Class<T> cls )
        {
            return (ProxyFactory<T>) INSTANCES.get( cls );
        }

        @SuppressWarnings( "unchecked" )
        ProxyFactory( Class<T> type ) throws LinkageError
        {
            this.type = type;
            try
            {
                this.constructor = (Constructor<? extends T>) Proxy
                        .getProxyClass( ConsistencyReporter.class.getClassLoader(), type )
                        .getConstructor( InvocationHandler.class );
                INSTANCES.put( type, this );
            }
            catch ( NoSuchMethodException e )
            {
                throw new LinkageError( "Cannot access Proxy constructor for " + type.getName(), e );
            }
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + asList( constructor.getDeclaringClass().getInterfaces() );
        }

        Class<?> type()
        {
            return type;
        }

        public T create( InvocationHandler handler )
        {
            try
            {
                return constructor.newInstance( handler );
            }
            catch ( Exception e )
            {
                throw new LinkageError( "Failed to create proxy instance", e );
            }
        }

        public static <T> ProxyFactory<T> create( Class<T> type )
        {
            return new ProxyFactory<>( type );
        }
    }
}
