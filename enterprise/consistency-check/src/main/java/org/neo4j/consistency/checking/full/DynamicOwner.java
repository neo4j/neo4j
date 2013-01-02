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
package org.neo4j.consistency.checking.full;

import static org.neo4j.consistency.store.RecordReference.SkippingReference.skipReference;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.PendingReferenceCheck;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.AbstractNameRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

abstract class DynamicOwner<RECORD extends AbstractBaseRecord> implements Owner
{
    static final ComparativeRecordChecker<DynamicRecord, AbstractBaseRecord, ConsistencyReport.DynamicConsistencyReport>
            ORPHAN_CHECK =
            new ComparativeRecordChecker<DynamicRecord, AbstractBaseRecord, ConsistencyReport.DynamicConsistencyReport>()
            {
                @Override
                public void checkReference( DynamicRecord record, AbstractBaseRecord ignored,
                                            ConsistencyReport.DynamicConsistencyReport report, RecordAccess records )
                {
                    report.orphanDynamicRecord();
                }
            };

    abstract RecordReference<RECORD> record( RecordAccess records );

    @Override
    public void checkOrphanage()
    {
        // default: do nothing
    }

    static class Property extends DynamicOwner<PropertyRecord>
            implements ComparativeRecordChecker<PropertyRecord, AbstractBaseRecord, ConsistencyReport.PropertyConsistencyReport>
    {
        private final long id;
        private final RecordType type;

        Property( RecordType type, PropertyRecord record )
        {
            this.type = type;
            this.id = record.getId();
        }

        @Override
        RecordReference<PropertyRecord> record( RecordAccess records )
        {
            return records.property( id );
        }

        @Override
        public void checkReference( PropertyRecord property, AbstractBaseRecord record,
                                    ConsistencyReport.PropertyConsistencyReport report, RecordAccess records )
        {
            if ( record instanceof PropertyRecord )
            {
                if ( type == RecordType.STRING_PROPERTY )
                {
                    report.stringMultipleOwners( (PropertyRecord) record );
                }
                else
                {
                    report.arrayMultipleOwners( (PropertyRecord) record );
                }
            }
            else if ( record instanceof DynamicRecord )
            {
                if ( type == RecordType.STRING_PROPERTY )
                {
                    report.stringMultipleOwners( (DynamicRecord) record );
                }
                else
                {
                    report.arrayMultipleOwners( (DynamicRecord) record );
                }
            }
        }
    }

    static class Dynamic extends DynamicOwner<DynamicRecord>
            implements ComparativeRecordChecker<DynamicRecord, AbstractBaseRecord, ConsistencyReport.DynamicConsistencyReport>
    {
        private final long id;
        private final RecordType type;

        Dynamic( RecordType type, DynamicRecord record )
        {
            this.type = type;
            this.id = record.getId();
        }

        @Override
        RecordReference<DynamicRecord> record( RecordAccess records )
        {
            switch ( type )
            {
            case STRING_PROPERTY:
                return records.string( id );
            case ARRAY_PROPERTY:
                return records.array( id );
            case PROPERTY_KEY_NAME:
                return records.propertyKeyName( (int)id );
            case RELATIONSHIP_LABEL_NAME:
                return records.relationshipLabelName( (int)id );
            default:
                return skipReference();
            }
        }

        @Override
        public void checkReference( DynamicRecord block, AbstractBaseRecord record,
                                    ConsistencyReport.DynamicConsistencyReport report, RecordAccess records )
        {
            if ( record instanceof PropertyRecord )
            {
                report.nextMultipleOwners( (PropertyRecord) record );
            }
            else if ( record instanceof DynamicRecord )
            {
                report.nextMultipleOwners( (DynamicRecord) record );
            }
            else if ( record instanceof RelationshipTypeRecord )
            {
                report.nextMultipleOwners( (RelationshipTypeRecord) record );
            }
            else if ( record instanceof PropertyIndexRecord )
            {
                report.nextMultipleOwners( (PropertyIndexRecord) record );
            }
        }
    }

    static abstract class NameOwner<RECORD extends AbstractNameRecord, REPORT extends ConsistencyReport.NameConsistencyReport<RECORD, REPORT>> extends DynamicOwner<RECORD>
            implements ComparativeRecordChecker<RECORD, AbstractBaseRecord, REPORT>
    {
        @Override
        public void checkReference( RECORD name, AbstractBaseRecord record, REPORT genericReport, RecordAccess records )
        {
            @SuppressWarnings("UnnecessaryLocalVariable" /* Intellij has issues compiling the generic type */)
            ConsistencyReport.NameConsistencyReport report = genericReport;
            if ( record instanceof RelationshipTypeRecord )
            {
                ((ConsistencyReport.LabelConsistencyReport) report)
                        .nameMultipleOwners( (RelationshipTypeRecord) record );
            }
            else if ( record instanceof PropertyIndexRecord )
            {
                ((ConsistencyReport.PropertyKeyConsistencyReport) report)
                        .nameMultipleOwners( (PropertyIndexRecord) record );
            }
            else if ( record instanceof DynamicRecord )
            {
                report.nameMultipleOwners( (DynamicRecord) record );
            }
        }
    }

    static class PropertyKey extends NameOwner<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport>
    {
        private final int id;

        PropertyKey( PropertyIndexRecord record )
        {
            this.id = record.getId();
        }

        @Override
        RecordReference<PropertyIndexRecord> record( RecordAccess records )
        {
            return records.propertyKey( id );
        }
    }

    static class RelationshipLabel extends NameOwner<RelationshipTypeRecord,ConsistencyReport.LabelConsistencyReport>
    {
        private final int id;

        RelationshipLabel( RelationshipTypeRecord record )
        {
            this.id = record.getId();
        }

        @Override
        RecordReference<RelationshipTypeRecord> record( RecordAccess records )
        {
            return records.relationshipLabel( id );
        }
    }

    static class Unknown extends DynamicOwner<AbstractBaseRecord> implements RecordReference<AbstractBaseRecord>
    {
        private PendingReferenceCheck<AbstractBaseRecord> reporter;

        @Override
        RecordReference<AbstractBaseRecord> record( RecordAccess records )
        {
            // Getting the record for this owner means that some other owner replaced it
            // that means that it isn't an orphan, so we skip this orphan check
            // and return a record for conflict check that always is ok (by skipping the check)
            this.markInCustody();
            return skipReference();
        }

        public void checkOrphanage()
        {
            PendingReferenceCheck<AbstractBaseRecord> reporter = pop();
            if ( reporter != null )
            {
                reporter.checkReference( null, null );
            }
        }

        void markInCustody()
        {
            PendingReferenceCheck<AbstractBaseRecord> reporter = pop();
            if ( reporter != null )
            {
                reporter.skip();
            }
        }

        private synchronized PendingReferenceCheck<AbstractBaseRecord> pop()
        {
            try
            {
                return this.reporter;
            }
            finally
            {
                this.reporter = null;
            }
        }

        @Override
        public synchronized void dispatch( PendingReferenceCheck<AbstractBaseRecord> reporter )
        {
            this.reporter = reporter;
        }
    }

    private DynamicOwner()
    {
        // only internal subclasses
    }
}
