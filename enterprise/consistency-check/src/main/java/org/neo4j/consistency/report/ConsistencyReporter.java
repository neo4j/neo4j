/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.report;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;

import static java.lang.reflect.Proxy.getInvocationHandler;
import static java.util.Arrays.asList;

import static org.neo4j.consistency.report.ConsistencyReport.DynamicLabelConsistencyReport;
import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.Exceptions.withCause;

public class ConsistencyReporter implements ConsistencyReport.Reporter
{
    private static final Method FOR_REFERENCE;

    static
    {
        try
        {
            FOR_REFERENCE = ConsistencyReport.class
                    .getDeclaredMethod( "forReference", RecordReference.class,
                                        ComparativeRecordChecker.class );
        }
        catch ( NoSuchMethodException cause )
        {
            throw withCause( new LinkageError( "Could not find dispatch method of " +
                                               ConsistencyReport.class.getName() ), cause );
        }
    }

    private static final ProxyFactory<ConsistencyReport.SchemaConsistencyReport> SCHEMA_REPORT =
            ProxyFactory.create( ConsistencyReport.SchemaConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.NodeConsistencyReport> NODE_REPORT =
            ProxyFactory.create( ConsistencyReport.NodeConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.RelationshipConsistencyReport> RELATIONSHIP_REPORT =
            ProxyFactory.create( ConsistencyReport.RelationshipConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.PropertyConsistencyReport> PROPERTY_REPORT =
            ProxyFactory.create( ConsistencyReport.PropertyConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.RelationshipTypeConsistencyReport> RELATIONSHIP_TYPE_REPORT =
            ProxyFactory.create( ConsistencyReport.RelationshipTypeConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.LabelTokenConsistencyReport> LABEL_KEY_REPORT =
            ProxyFactory.create( ConsistencyReport.LabelTokenConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.PropertyKeyTokenConsistencyReport> PROPERTY_KEY_REPORT =
            ProxyFactory.create( ConsistencyReport.PropertyKeyTokenConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.DynamicConsistencyReport> DYNAMIC_REPORT =
            ProxyFactory.create( ConsistencyReport.DynamicConsistencyReport.class );
    private static final ProxyFactory<ConsistencyReport.DynamicLabelConsistencyReport> DYNAMIC_LABEL_REPORT =
            ProxyFactory.create( ConsistencyReport.DynamicLabelConsistencyReport.class );

    private final DiffRecordAccess records;
    private final InconsistencyReport report;

    public ConsistencyReporter( DiffRecordAccess records, InconsistencyReport report )
    {
        this.records = records;
        this.report = report;
    }

    private <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    void dispatch( RecordType type, ProxyFactory<REPORT> factory, RECORD record, RecordCheck<RECORD, REPORT> checker )
    {
        ReportHandler handler = new ReportHandler( report, type, record );
        checker.check( record, factory.create( handler ), records );
        handler.updateSummary();
    }

    private <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    void dispatchChange( RecordType type, ProxyFactory<REPORT> factory, RECORD oldRecord, RECORD newRecord,
                         RecordCheck<RECORD, REPORT> checker )
    {
        DiffReportHandler handler = new DiffReportHandler( report, type, oldRecord, newRecord );
        checker.checkChange( oldRecord, newRecord, factory.create( handler ), records );
        handler.updateSummary();
    }

    static void dispatchReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                   AbstractBaseRecord referenced, RecordAccess records )
    {
        ReportInvocationHandler handler = (ReportInvocationHandler) getInvocationHandler( report );
        handler.checkReference( report, checker, referenced, records );
        handler.updateSummary();
    }

    static String pendingCheckToString(ConsistencyReport report,ComparativeRecordChecker checker)
    {
        ReportInvocationHandler handler = (ReportInvocationHandler) getInvocationHandler( report );
        return handler.pendingCheckToString(checker);
    }

    static void dispatchChangeReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                         AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced,
                                         RecordAccess records )
    {
        ReportInvocationHandler handler = (ReportInvocationHandler) getInvocationHandler( report );
        handler.checkDiffReference( report, checker, oldReferenced, newReferenced, records );
        handler.updateSummary();
    }

    static void dispatchSkip( ConsistencyReport report )
    {
        ((ReportInvocationHandler) getInvocationHandler( report )).updateSummary();
    }

    private static abstract class ReportInvocationHandler implements InvocationHandler
    {
        final InconsistencyReport report;
        final RecordType type;
        private short errors = 0, warnings = 0, references = 1/*this*/;

        private ReportInvocationHandler( InconsistencyReport report, RecordType type )
        {
            this.report = report;
            this.type = type;
        }

        synchronized void updateSummary()
        {
            if ( --references == 0 )
            {
                report.updateSummary( type, errors, warnings );
            }
        }

        String pendingCheckToString( ComparativeRecordChecker checker )
        {
            String checkName;
            try
            {
                if ( checker.getClass().getMethod( "toString" ).getDeclaringClass() == Object.class )
                {
                    checkName = checker.getClass().getSimpleName();
                    if ( checkName.length() == 0 )
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

        /**
         * Invoked when an inconsistency is encountered.
         *
         * @param args array of the items referenced from this record with which it is inconsistent.
         */
        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            if ( method.equals( FOR_REFERENCE ) )
            {
                RecordReference reference = (RecordReference) args[0];
                ComparativeRecordChecker checker = (ComparativeRecordChecker) args[1];
                dispatchForReference( (ConsistencyReport) proxy, reference, checker );
            }
            else
            {
                String message;
                Documented annotation = method.getAnnotation( Documented.class );
                if ( annotation != null && !"".equals( annotation.value() ) )
                {
                   message = annotation.value();
                }
                else
                {
                    message = method.getName();
                }
                if ( method.getAnnotation( ConsistencyReport.Warning.class ) == null )
                {
                    errors++;
                    logError( message, args );
                }
                else
                {
                    warnings++;
                    logWarning( message, args );
                }
            }
            return null;
        }

        protected abstract void logError( String message, Object[] args );

        protected abstract void logWarning( String message, Object[] args );

        @SuppressWarnings("unchecked")
        private void dispatchForReference( ConsistencyReport report, RecordReference reference,
                                   ComparativeRecordChecker checker )
        {
            forReference( report, reference, checker );
        }

        final <REFERENCED extends AbstractBaseRecord>
        void forReference( ConsistencyReport report, RecordReference<REFERENCED> reference,
                           ComparativeRecordChecker<?, REFERENCED, ?> checker )
        {
            references++;
            reference.dispatch( new PendingReferenceCheck<REFERENCED>( report, checker ) );
        }

        abstract void checkReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                      AbstractBaseRecord referenced, RecordAccess records );

        abstract void checkDiffReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                          AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced,
                                          RecordAccess records );
    }

    static class ReportHandler extends ReportInvocationHandler
    {
        private final AbstractBaseRecord record;

        ReportHandler( InconsistencyReport report, RecordType type,
            AbstractBaseRecord record )
            {
                super( report, type );
                this.record = record;
        }

        @Override
        long recordId()
        {
            return record.getLongId();
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
        @SuppressWarnings("unchecked")
        void checkReference( ConsistencyReport report, ComparativeRecordChecker checker, AbstractBaseRecord referenced,
                             RecordAccess records )
        {
            checker.checkReference( record, referenced, report, records );
        }

        @Override
        @SuppressWarnings("unchecked")
        void checkDiffReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                 AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced,
                                 RecordAccess records )
        {
            checker.checkReference( record, newReferenced, report, records );
        }
    }

    private static class DiffReportHandler extends ReportInvocationHandler
    {
        private final AbstractBaseRecord oldRecord;
        private final AbstractBaseRecord newRecord;

        private DiffReportHandler( InconsistencyReport report, RecordType type,
                                   AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord )
        {
            super( report, type );
            this.oldRecord = oldRecord;
            this.newRecord = newRecord;
        }

        @Override
        long recordId()
        {
            return newRecord.getLongId();
        }

        @Override
        protected void logError( String message, Object[] args )
        {
            report.error( type, oldRecord, newRecord, message, args );
        }

        @Override
        protected void logWarning( String message, Object[] args )
        {
            report.warning( type, oldRecord, newRecord, message, args );
        }

        @Override
        @SuppressWarnings("unchecked")
        void checkReference( ConsistencyReport report, ComparativeRecordChecker checker, AbstractBaseRecord referenced,
                             RecordAccess records )
        {
            checker.checkReference( newRecord, referenced, report, records );
        }

        @Override
        @SuppressWarnings("unchecked")
        void checkDiffReference( ConsistencyReport report, ComparativeRecordChecker checker,
                                 AbstractBaseRecord oldReferenced, AbstractBaseRecord newReferenced,
                                 RecordAccess records )
        {
            checker.checkReference( newRecord, newReferenced, report, records );
        }
    }

    @Override
    public void forSchema( DynamicRecord schema,
                           RecordCheck<DynamicRecord, ConsistencyReport.SchemaConsistencyReport> checker )
    {
        dispatch( RecordType.SCHEMA, SCHEMA_REPORT, schema, checker );
    }

    @Override
    public void forSchemaChange( DynamicRecord oldSchema, DynamicRecord newSchema, RecordCheck<DynamicRecord,
            ConsistencyReport.SchemaConsistencyReport> checker )
    {
        dispatchChange( RecordType.SCHEMA, SCHEMA_REPORT, oldSchema, newSchema, checker );
    }

    @Override
    public void forNode( NodeRecord node,
                         RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
        dispatch( RecordType.NODE, NODE_REPORT, node, checker );
    }

    @Override
    public void forNodeChange( NodeRecord oldNode, NodeRecord newNode,
                               RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
        dispatchChange( RecordType.NODE, NODE_REPORT, oldNode, newNode, checker );
    }

    @Override
    public void forRelationship( RelationshipRecord relationship,
                                 RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        dispatch( RecordType.RELATIONSHIP, RELATIONSHIP_REPORT, relationship, checker );
    }

    @Override
    public void forRelationshipChange( RelationshipRecord oldRelationship, RelationshipRecord newRelationship,
                                       RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        dispatchChange( RecordType.RELATIONSHIP, RELATIONSHIP_REPORT, oldRelationship, newRelationship, checker );
    }

    @Override
    public void forProperty( PropertyRecord property,
                             RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
        dispatch( RecordType.PROPERTY, PROPERTY_REPORT, property, checker );
    }

    @Override
    public void forPropertyChange( PropertyRecord oldProperty, PropertyRecord newProperty,
                                   RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
        dispatchChange( RecordType.PROPERTY, PROPERTY_REPORT, oldProperty, newProperty, checker );
    }

    @Override
    public void forRelationshipTypeName( RelationshipTypeTokenRecord relationshipTypeTokenRecord,
                                         RecordCheck<RelationshipTypeTokenRecord,
                                                 ConsistencyReport.RelationshipTypeConsistencyReport> checker )
    {
        dispatch( RecordType.RELATIONSHIP_TYPE, RELATIONSHIP_TYPE_REPORT, relationshipTypeTokenRecord, checker );
    }

    @Override
    public void forRelationshipTypeNameChange( RelationshipTypeTokenRecord oldType, RelationshipTypeTokenRecord newType,
                                               RecordCheck<RelationshipTypeTokenRecord,
                                                       ConsistencyReport.RelationshipTypeConsistencyReport> checker )
    {
        dispatchChange( RecordType.RELATIONSHIP_TYPE, RELATIONSHIP_TYPE_REPORT, oldType, newType, checker );
    }

    @Override
    public void forLabelName( LabelTokenRecord label, RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> checker )
    {
        dispatch( RecordType.LABEL, LABEL_KEY_REPORT, label, checker );
    }

    @Override
    public void forLabelNameChange( LabelTokenRecord oldLabel, LabelTokenRecord newLabel, RecordCheck<LabelTokenRecord,
            ConsistencyReport.LabelTokenConsistencyReport> checker )
    {
        dispatchChange( RecordType.LABEL, LABEL_KEY_REPORT, oldLabel, newLabel, checker );
    }

    @Override
    public void forPropertyKey( PropertyKeyTokenRecord key,
                                RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
    {
        dispatch( RecordType.PROPERTY_KEY, PROPERTY_KEY_REPORT, key, checker );
    }

    @Override
    public void forPropertyKeyChange( PropertyKeyTokenRecord oldKey, PropertyKeyTokenRecord newKey,
                                      RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
    {
        dispatchChange( RecordType.PROPERTY_KEY, PROPERTY_KEY_REPORT, oldKey, newKey, checker );
    }

    @Override
    public void forDynamicBlock( RecordType type, DynamicRecord record,
                                 RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
    {
        dispatch( type, DYNAMIC_REPORT, record, checker );
    }

    @Override
    public void forDynamicBlockChange( RecordType type, DynamicRecord oldRecord, DynamicRecord newRecord,
                                       RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
    {
        dispatchChange( type, DYNAMIC_REPORT, oldRecord, newRecord, checker );
    }

    @Override
    public void forDynamicLabelBlock( RecordType type, DynamicRecord record,
                                      RecordCheck<DynamicRecord, DynamicLabelConsistencyReport> checker )
    {
        dispatch( type, DYNAMIC_LABEL_REPORT, record, checker );
    }

    @Override
    public void forDynamicLabelBlockChange( RecordType type, DynamicRecord oldRecord, DynamicRecord newRecord,
                                            RecordCheck<DynamicRecord, DynamicLabelConsistencyReport> checker )
    {
        dispatchChange( type, DYNAMIC_LABEL_REPORT, oldRecord, newRecord, checker );
    }

    private static class ProxyFactory<T>
    {
        private Constructor<? extends T> constructor;

        @SuppressWarnings("unchecked")
        ProxyFactory( Class<T> type ) throws LinkageError
        {
            try
            {
                this.constructor = (Constructor<? extends T>) Proxy
                        .getProxyClass( ConsistencyReporter.class.getClassLoader(), type )
                        .getConstructor( InvocationHandler.class );
            }
            catch ( NoSuchMethodException e )
            {
                throw withCause( new LinkageError( "Cannot access Proxy constructor for " + type.getName() ), e );
            }
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + asList( constructor.getDeclaringClass().getInterfaces() );
        }

        public T create( InvocationHandler handler )
        {
            try
            {
                return constructor.newInstance( handler );
            }
            catch ( InvocationTargetException e )
            {
                throw launderedException( e );
            }
            catch ( Exception e )
            {
                throw new LinkageError( "Failed to create proxy instance" );
            }
        }

        public static <T> ProxyFactory<T> create( Class<T> type )
        {
            return new ProxyFactory<>( type );
        }
    }
}
