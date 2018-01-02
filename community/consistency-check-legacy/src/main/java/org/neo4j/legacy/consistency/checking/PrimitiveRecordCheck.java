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

import java.util.Arrays;

import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;

public abstract class PrimitiveRecordCheck
        <RECORD extends PrimitiveRecord, REPORT extends ConsistencyReport.PrimitiveConsistencyReport>
        implements OwningRecordCheck<RECORD, REPORT>
{
    private final RecordField<RECORD, REPORT>[] fields;
    private final ComparativeRecordChecker<RECORD, PrimitiveRecord, REPORT> ownerCheck =
            new ComparativeRecordChecker<RECORD, PrimitiveRecord, REPORT>()
            {
                @Override
                public void checkReference( RECORD record, PrimitiveRecord other, CheckerEngine<RECORD, REPORT> engine,
                                            RecordAccess records )
                {
                    if ( record.getId() == other.getId() && record.getClass() == other.getClass() )
                    {
                        // Owner identities match. Things are as they should be.
                        return;
                    }

                    if ( other instanceof NodeRecord )
                    {
                        engine.report().multipleOwners( (NodeRecord) other );
                    }
                    else if ( other instanceof RelationshipRecord )
                    {
                        engine.report().multipleOwners( (RelationshipRecord) other );
                    }
                    else if ( other instanceof NeoStoreRecord )
                    {
                        engine.report().multipleOwners( (NeoStoreRecord) other );
                    }
                }
            };

    @SafeVarargs
    PrimitiveRecordCheck( RecordField<RECORD, REPORT>... fields )
    {
        this.fields = Arrays.copyOf( fields, fields.length + 1 );
        this.fields[fields.length] = new FirstProperty();
    }

    private class FirstProperty
            implements RecordField<RECORD, REPORT>, ComparativeRecordChecker<RECORD, PropertyRecord, REPORT>
    {
        @Override
        public void checkConsistency( RECORD record, CheckerEngine<RECORD, REPORT> engine,
                                      RecordAccess records )
        {
            if ( !Record.NO_NEXT_PROPERTY.is( record.getNextProp() ) )
            {
                engine.comparativeCheck( records.property( record.getNextProp() ), this );
            }
        }

        @Override
        public long valueFrom( RECORD record )
        {
            return record.getNextProp();
        }

        @Override
        public void checkChange( RECORD oldRecord, RECORD newRecord, CheckerEngine<RECORD, REPORT> engine,
                                 DiffRecordAccess records )
        {
            if ( !newRecord.inUse() || valueFrom( oldRecord ) != valueFrom( newRecord ) )
            {
                if ( !Record.NO_NEXT_PROPERTY.is( valueFrom( oldRecord ) )
                     && records.changedProperty( valueFrom( oldRecord ) ) == null )
                {
                    engine.report().propertyNotUpdated();
                }
            }
        }

        @Override
        public void checkReference( RECORD record, PropertyRecord property, CheckerEngine<RECORD, REPORT> engine,
                                    RecordAccess records )
        {
            if ( !property.inUse() )
            {
                engine.report().propertyNotInUse( property );
            }
            else
            {
                if ( !Record.NO_PREVIOUS_PROPERTY.is( property.getPrevProp() ) )
                {
                    engine.report().propertyNotFirstInChain( property );
                }
                new ChainCheck<RECORD, REPORT>().checkReference( record, property, engine, records );
            }
        }
    }

    @Override
    public void check( RECORD record, CheckerEngine<RECORD, REPORT> engine, RecordAccess records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        for ( RecordField<RECORD, REPORT> field : fields )
        {
            field.checkConsistency( record, engine, records );
        }
    }

    @Override
    public void checkChange( RECORD oldRecord, RECORD newRecord, CheckerEngine<RECORD, REPORT> engine,
                             DiffRecordAccess records )
    {
        check( newRecord, engine, records );
        if ( oldRecord.inUse() )
        {
            for ( RecordField<RECORD, REPORT> field : fields )
            {
                field.checkChange( oldRecord, newRecord, engine, records );
            }
        }
    }

    @Override
    public ComparativeRecordChecker<RECORD,PrimitiveRecord,REPORT> ownerCheck()
    {
        return ownerCheck;
    }
}
