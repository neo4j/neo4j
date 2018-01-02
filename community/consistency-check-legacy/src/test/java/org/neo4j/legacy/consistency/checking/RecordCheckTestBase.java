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
package org.neo4j.legacy.consistency.checking;

import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccessStub;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public abstract class RecordCheckTestBase<RECORD extends AbstractBaseRecord,
        REPORT extends ConsistencyReport,
        CHECKER extends RecordCheck<RECORD, REPORT>>
{
    public static final int NONE = -1;
    protected final CHECKER checker;
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
            public void check( NodeRecord record,
                               CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                                     CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                                     DiffRecordAccess records )
            {
            }
        };
    }

    public static PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> dummyRelationshipChecker()
    {
        return new RelationshipRecordCheck()
        {
            @Override
            public void check( RelationshipRecord record,
                               CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                                     CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
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
            public void check( PropertyRecord record,
                               CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                                     CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                     DiffRecordAccess records )
            {
            }
        };
    }

    public static PrimitiveRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> dummyNeoStoreCheck()
    {
        return new NeoStoreCheck()
        {
            @Override
            public void check( NeoStoreRecord record,
                               CheckerEngine<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> engine,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( NeoStoreRecord oldRecord, NeoStoreRecord newRecord,
                                     CheckerEngine<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> engine,
                                     DiffRecordAccess records )
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
            public void check( DynamicRecord record,
                               CheckerEngine<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> engine,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord,
                                     CheckerEngine<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> engine,
                                     DiffRecordAccess records )
            {
            }
        };
    }

    public static RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> dummyPropertyKeyCheck()
    {
        return new PropertyKeyTokenRecordCheck()
        {
            @Override
            public void check( PropertyKeyTokenRecord record,
                               CheckerEngine<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> engine,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( PropertyKeyTokenRecord oldRecord, PropertyKeyTokenRecord newRecord,
                                     CheckerEngine<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> engine,
                                     DiffRecordAccess records )
            {
            }
        };
    }

    public static RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> dummyRelationshipLabelCheck()
    {
        return new RelationshipTypeTokenRecordCheck()
        {
            @Override
            public void check( RelationshipTypeTokenRecord record,
                               CheckerEngine<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> engine,
                               RecordAccess records )
            {
            }

            @Override
            public void checkChange( RelationshipTypeTokenRecord oldRecord, RelationshipTypeTokenRecord newRecord,
                                     CheckerEngine<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> engine,
                                     DiffRecordAccess records )
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

    public static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport>
    REPORT check( Class<REPORT> reportClass, RecordCheck<RECORD, REPORT> checker, RECORD record,
                  final RecordAccessStub records )
    {
        REPORT report = mock( reportClass );
        checker.check( record, records.engine( record, report ), records );
        records.checkDeferred();
        return report;
    }

    static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport>
    REPORT checkChange( Class<REPORT> reportClass, RecordCheck<RECORD, REPORT> checker,
                        RECORD oldRecord, RECORD newRecord, final RecordAccessStub records )
    {
        REPORT report = mock( reportClass );
        checker.checkChange( oldRecord, newRecord, records.engine( oldRecord, newRecord, report ), records );
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

    protected CHECKER checker()
    {
        return checker;
    }
}
