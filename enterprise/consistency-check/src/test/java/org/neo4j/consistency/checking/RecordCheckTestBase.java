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
package org.neo4j.consistency.checking;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordAccessStub;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public abstract class RecordCheckTestBase<RECORD extends AbstractBaseRecord,
        REPORT extends ConsistencyReport<RECORD, REPORT>,
        CHECKER extends RecordCheck<RECORD, REPORT>>
{
    public static final int NONE = -1;
    private final CHECKER checker;
    private final Class<REPORT> reportClass;
    protected final RecordAccessStub records = new RecordAccessStub();

    RecordCheckTestBase( CHECKER checker, Class<REPORT> reportClass )
    {
        this.checker = checker;
        this.reportClass = reportClass;
    }

    public static PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> dummyNodeCheck()
    {
        return new NodeRecordCheck()
        {
            @Override
            public void check( NodeRecord record, ConsistencyReport.NodeConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                                     ConsistencyReport.NodeConsistencyReport report, DiffRecordAccess records )
            {
            }
        };
    }

    public static PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> dummyRelationshipChecker()
    {
        return new RelationshipRecordCheck()
        {
            @Override
            public void check( RelationshipRecord record, ConsistencyReport.RelationshipConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                                     ConsistencyReport.RelationshipConsistencyReport report,
                                     DiffRecordAccess records )
            {
            }
        };
    }

    public static RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> dummyPropertyChecker()
    {
        return new RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>()
        {
            @Override
            public void check( PropertyRecord record, ConsistencyReport.PropertyConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                                     ConsistencyReport.PropertyConsistencyReport report, DiffRecordAccess records )
            {
            }
        };
    }

    public static PrimitiveRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> dummyNeoStoreCheck()
    {
        return new NeoStoreCheck()
        {
            @Override
            public void check( NeoStoreRecord record, ConsistencyReport.NeoStoreConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( NeoStoreRecord oldRecord, NeoStoreRecord newRecord,
                                     ConsistencyReport.NeoStoreConsistencyReport report, DiffRecordAccess records )
            {
            }
        };
    }

    public static RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> dummyDynamicCheck(
            RecordStore<DynamicRecord> store, DynamicStore dereference )
    {
        return new DynamicRecordCheck(store, dereference )
        {
            @Override
            public void check( DynamicRecord record, ConsistencyReport.DynamicConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord,
                                     ConsistencyReport.DynamicConsistencyReport report, DiffRecordAccess records )
            {
            }
        };
    }

    public static RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> dummyPropertyKeyCheck()
    {
        return new PropertyKeyTokenRecordCheck()
        {
            @Override
            public void check( PropertyKeyTokenRecord record, ConsistencyReport.PropertyKeyTokenConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( PropertyKeyTokenRecord oldRecord, PropertyKeyTokenRecord newRecord,
                                     ConsistencyReport.PropertyKeyTokenConsistencyReport report, DiffRecordAccess records )
            {
            }
        };
    }

    public static RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> dummyRelationshipLabelCheck()
    {
        return new RelationshipTypeTokenRecordCheck()
        {
            @Override
            public void check( RelationshipTypeTokenRecord record, ConsistencyReport.RelationshipTypeConsistencyReport report,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( RelationshipTypeTokenRecord oldRecord, RelationshipTypeTokenRecord newRecord,
                                     ConsistencyReport.RelationshipTypeConsistencyReport report, DiffRecordAccess records )
            {
            }
        };
    }

    final REPORT check( RECORD record )
    {
        return check( reportClass, checker, record, records );
    }

    final REPORT check( CHECKER externalChecker, RECORD record )
    {
        return check( reportClass, externalChecker, record, records );
    }

    final REPORT checkChange( RECORD oldRecord, RECORD newRecord )
    {
        return checkChange( reportClass, checker, oldRecord, newRecord, records );
    }

    public static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    REPORT check( Class<REPORT> reportClass, RecordCheck<RECORD, REPORT> checker, RECORD record,
                  final RecordAccessStub records )
    {
        REPORT report = records.mockReport( reportClass, record );
        checker.check( record, report, records );
        records.checkDeferred();
        return report;
    }

    static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    REPORT checkChange( Class<REPORT> reportClass, RecordCheck<RECORD, REPORT> checker,
                        RECORD oldRecord, RECORD newRecord, final RecordAccessStub records )
    {
        REPORT report = records.mockReport( reportClass, oldRecord, newRecord );
        checker.checkChange( oldRecord, newRecord, report, records );
        records.checkDeferred();
        return report;
    }

    <R extends AbstractBaseRecord> R addChange( R oldRecord, R newRecord )
    {
        return records.addChange( oldRecord, newRecord );
    }

    <R extends AbstractBaseRecord> R add( R record )
    {
        return records.add( record );
    }

    DynamicRecord addNodeDynamicLabels( DynamicRecord labels )
    {
        return records.addNodeDynamicLabels( labels );
    }

    DynamicRecord addKeyName( DynamicRecord name )
    {
        return records.addPropertyKeyName( name );
    }

    DynamicRecord addRelationshipTypeName(DynamicRecord name )
    {
        return records.addRelationshipTypeName( name );
    }

    DynamicRecord addLabelName( DynamicRecord name )
    {
        return records.addLabelName( name );
    }

    public static DynamicRecord string( DynamicRecord record )
    {
        record.setType( PropertyType.STRING.intValue() );
        return record;
    }

    public static DynamicRecord array( DynamicRecord record )
    {
        record.setType( PropertyType.ARRAY.intValue() );
        return record;
    }

    static PropertyBlock propertyBlock( PropertyKeyTokenRecord key, DynamicRecord value )
    {
        PropertyType type;
        if ( value.getType() == PropertyType.STRING.intValue() )
        {
            type = PropertyType.STRING;
        }
        else if ( value.getType() == PropertyType.ARRAY.intValue() )
        {
            type = PropertyType.ARRAY;
        }
        else
        {
            fail( "Dynamic record must be either STRING or ARRAY" );
            return null;
        }
        return propertyBlock( key, type, value.getId() );
    }

    public static PropertyBlock propertyBlock( PropertyKeyTokenRecord key, PropertyType type, long value )
    {
        PropertyBlock block = new PropertyBlock();
        block.setSingleBlock( key.getId() | (((long) type.intValue()) << 24) | (value << 28) );
        return block;
    }

    public static <R extends AbstractBaseRecord> R inUse( R record )
    {
        record.setInUse( true );
        return record;
    }

    public static <R extends AbstractBaseRecord> R notInUse( R record )
    {
        record.setInUse( false );
        return record;
    }

    @SuppressWarnings("unchecked")
    public static void verifyOnlyReferenceDispatch( ConsistencyReport report )
    {
        verify( report, atLeast( 0 ) )
                .forReference( any( RecordReference.class ), any( ComparativeRecordChecker.class ) );
        verifyNoMoreInteractions( report );
    }

    protected CHECKER checker()
    {
        return checker;
    }
}
