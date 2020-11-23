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

import org.neo4j.annotations.documented.DocumentedUtils;
import org.neo4j.annotations.documented.Warning;
import org.neo4j.consistency.RecordType;
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
import org.neo4j.consistency.store.synthetic.CountsEntry;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.TokenScanDocument;
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

public class ConsistencyReporter implements ConsistencyReport.Reporter
{
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

    private final InconsistencyReport report;
    private final Monitor monitor;

    public interface Monitor
    {
        void reported( Class<?> report, String method, String message );
    }

    public static final Monitor NO_MONITOR = ( report, method, message ) ->
    {
    };

    public ConsistencyReporter( InconsistencyReport report )
    {
        this( report, NO_MONITOR );
    }

    public ConsistencyReporter( InconsistencyReport report, Monitor monitor )
    {
        this.report = report;
        this.monitor = monitor;
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

    public abstract static class ReportInvocationHandler<REPORT extends ConsistencyReport> implements InvocationHandler
    {
        final InconsistencyReport report;
        private final ProxyFactory<REPORT> factory;
        final RecordType type;
        private final Monitor monitor;

        private ReportInvocationHandler( InconsistencyReport report, ProxyFactory<REPORT> factory, RecordType type, Monitor monitor )
        {
            this.report = report;
            this.factory = factory;
            this.type = type;
            this.monitor = monitor;
        }

        REPORT report()
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
            if ( method.getName().equals( "toString" ) )
            {
                return factory.type.getName();
            }

            String message = DocumentedUtils.extractMessage( method );
            if ( method.getAnnotation( Warning.class ) == null )
            {
                logError( message, args );
                report.updateSummary( type, 1, 0 );
            }
            else
            {
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

        protected abstract void logError( String message, Object... args );

        protected abstract void logWarning( String message, Object... args );
    }

    public static class ReportHandler<REPORT extends ConsistencyReport> extends ReportInvocationHandler<REPORT>
    {
        private final AbstractBaseRecord record;

        public ReportHandler( InconsistencyReport report, ProxyFactory<REPORT> factory, RecordType type, AbstractBaseRecord record, Monitor monitor )
        {
            super( report, factory, type, monitor );
            this.record = record;
        }

        @Override
        protected void logError( String message, Object... args )
        {
            report.error( type, record, message, args );
        }

        @Override
        protected void logWarning( String message, Object... args )
        {
            report.warning( type, record, message, args );
        }
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
        return new ReportHandler<>( report, factory, type, record, monitor ).report();
    }

    public static class ProxyFactory<T>
    {
        private final Constructor<? extends T> constructor;
        private final Class<T> type;

        @SuppressWarnings( "unchecked" )
        ProxyFactory( Class<T> type ) throws LinkageError
        {
            this.type = type;
            try
            {
                this.constructor = (Constructor<? extends T>) Proxy
                        .getProxyClass( ConsistencyReporter.class.getClassLoader(), type )
                        .getConstructor( InvocationHandler.class );
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
