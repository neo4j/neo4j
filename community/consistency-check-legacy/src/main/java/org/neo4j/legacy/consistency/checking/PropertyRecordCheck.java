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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;

class PropertyRecordCheck
        implements RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
{
    @Override
    public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                             CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                             DiffRecordAccess records )
    {
        check( newRecord, engine, records );
        if ( oldRecord.inUse() )
        {
            for ( PropertyField field : PropertyField.values() )
            {
                field.checkChange( oldRecord, newRecord, engine, records );
            }
        }
        // Did this record belong to the marked owner before it changed? - does it belong to it now?
        if ( oldRecord.inUse() )
        {
            OwnerChain.OLD.check( newRecord, engine, records );
        }
        if ( newRecord.inUse() )
        {
            OwnerChain.NEW.check( newRecord, engine, records );
        }
        // Previously referenced dynamic records should either still be referenced, or be deleted
        Map<Long, PropertyBlock> prevStrings = new HashMap<>();
        Map<Long, PropertyBlock> prevArrays = new HashMap<>();
        for ( PropertyBlock block : oldRecord )
        {
            PropertyType type = block.getType();
            if ( type != null )
            {
                switch ( type )
                {
                case STRING:
                    prevStrings.put( block.getSingleValueLong(), block );
                    break;
                case ARRAY:
                    prevArrays.put( block.getSingleValueLong(), block );
                    break;
                }
            }
        }
        for ( PropertyBlock block : newRecord )
        {
            PropertyType type = block.getType();
            if ( type != null )
            {
                switch ( type )
                {
                case STRING:
                    prevStrings.remove( block.getSingleValueLong() );
                    break;
                case ARRAY:
                    prevArrays.remove( block.getSingleValueLong() );
                    break;
                }
            }
        }
        for ( PropertyBlock block : prevStrings.values() )
        {
            if ( records.changedString( block.getSingleValueLong() ) == null )
            {
                engine.report().stringUnreferencedButNotDeleted( block );
            }
        }
        for ( PropertyBlock block : prevArrays.values() )
        {
            if ( records.changedArray( block.getSingleValueLong() ) == null )
            {
                engine.report().arrayUnreferencedButNotDeleted( block );
            }
        }
    }

    @Override
    public void check( PropertyRecord record,
                       CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                       RecordAccess records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        for ( PropertyField field : PropertyField.values() )
        {
            field.checkConsistency( record, engine, records );
        }
        for ( PropertyBlock block : record )
        {
            checkDataBlock( block, engine, records );
        }
    }

    private void checkDataBlock( PropertyBlock block,
                                 CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                 RecordAccess records )
    {
        if ( block.getKeyIndexId() < 0 )
        {
            engine.report().invalidPropertyKey( block );
        }
        else
        {
            engine.comparativeCheck( records.propertyKey( block.getKeyIndexId() ), propertyKey( block ) );
        }
        PropertyType type = block.forceGetType();
        if ( type == null )
        {
            engine.report().invalidPropertyType( block );
        }
        else
        {
            switch ( type )
            {
            case STRING:
                engine.comparativeCheck( records.string( block.getSingleValueLong() ),
                                         DynamicReference.string( block ) );
                break;
            case ARRAY:
                engine.comparativeCheck( records.array( block.getSingleValueLong() ), DynamicReference.array( block ) );
                break;
            default:
                try
                {
                    type.getValue( block, null );
                }
                catch ( Exception e )
                {
                    engine.report().invalidPropertyValue( block );
                }
                break;
            }
        }
    }

    private enum PropertyField implements
            RecordField<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>,
            ComparativeRecordChecker<PropertyRecord, PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
    {
        PREV( Record.NO_PREVIOUS_PROPERTY )
        {
            @Override
            public long valueFrom( PropertyRecord record )
            {
                return record.getPrevProp();
            }

            @Override
            long otherReference( PropertyRecord record )
            {
                return record.getNextProp();
            }

            @Override
            void notInUse( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.prevNotInUse( property );
            }

            @Override
            void noBackReference( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.previousDoesNotReferenceBack( property );
            }

            @Override
            void reportNotUpdated( ConsistencyReport.PropertyConsistencyReport report )
            {
                report.prevNotUpdated();
            }
        },
        NEXT( Record.NO_NEXT_PROPERTY )
        {
            @Override
            public long valueFrom( PropertyRecord record )
            {
                return record.getNextProp();
            }

            @Override
            long otherReference( PropertyRecord record )
            {
                return record.getPrevProp();
            }

            @Override
            void notInUse( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.nextNotInUse( property );
            }

            @Override
            void noBackReference( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.nextDoesNotReferenceBack( property );
            }

            @Override
            void reportNotUpdated( ConsistencyReport.PropertyConsistencyReport report )
            {
                report.nextNotUpdated();
            }
        };
        private final Record NONE;

        private PropertyField( Record none )
        {
            this.NONE = none;
        }

        abstract long otherReference( PropertyRecord record );

        @Override
        public void checkConsistency( PropertyRecord record,
                                      CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                      RecordAccess records )
        {
            if ( !NONE.is( valueFrom( record ) ) )
            {
                engine.comparativeCheck( records.property( valueFrom( record ) ), this );
            }
        }

        @Override
        public void checkReference( PropertyRecord record, PropertyRecord referred,
                                    CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                    RecordAccess records )
        {
            if ( !referred.inUse() )
            {
                notInUse( engine.report(), referred );
            }
            else
            {
                if ( otherReference( referred ) != record.getId() )
                {
                    noBackReference( engine.report(), referred );
                }
            }
        }
        @Override
        public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                                 CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                 DiffRecordAccess records )
        {
            if ( !newRecord.inUse() || valueFrom( oldRecord ) != valueFrom( newRecord ) )
            {
                if ( !NONE.is( valueFrom( oldRecord ) )
                     && records.changedProperty( valueFrom( oldRecord ) ) == null )
                {
                    reportNotUpdated( engine.report() );
                }
            }
        }

        abstract void reportNotUpdated( ConsistencyReport.PropertyConsistencyReport report );

        abstract void notInUse( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property );

        abstract void noBackReference( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property );
    }

    private static ComparativeRecordChecker<PropertyRecord, PropertyKeyTokenRecord, ConsistencyReport.PropertyConsistencyReport>
    propertyKey( final PropertyBlock block )
    {
        return new ComparativeRecordChecker<PropertyRecord, PropertyKeyTokenRecord, ConsistencyReport.PropertyConsistencyReport>()
        {
            @Override
            public void checkReference( PropertyRecord record, PropertyKeyTokenRecord referred,
                                        CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                        RecordAccess records )
            {
                if ( !referred.inUse() )
                {
                    engine.report().keyNotInUse( block, referred );
                }
            }

            @Override
            public String toString()
            {
                return "PROPERTY_KEY";
            }
        };
    }

    private static abstract class DynamicReference implements
            ComparativeRecordChecker<PropertyRecord, DynamicRecord, ConsistencyReport.PropertyConsistencyReport>
    {
        final PropertyBlock block;

        private DynamicReference( PropertyBlock block )
        {
            this.block = block;
        }

        public static DynamicReference string( PropertyBlock block )
        {
            return new DynamicReference( block )
            {
                @Override
                void notUsed( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.stringNotInUse( block, value );
                }

                @Override
                void empty( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.stringEmpty( block, value );
                }
            };
        }

        public static DynamicReference array( PropertyBlock block )
        {
            return new DynamicReference( block )
            {
                @Override
                void notUsed( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.arrayNotInUse( block, value );
                }

                @Override
                void empty( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.arrayEmpty( block, value );
                }
            };
        }

        @Override
        public void checkReference( PropertyRecord record, DynamicRecord referred,
                                    CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                    RecordAccess records )
        {
            if ( !referred.inUse() )
            {
                notUsed( engine.report(), referred );
            }
            else
            {
                if ( referred.getLength() <= 0 )
                {
                    empty( engine.report(), referred );
                }
            }
        }

        abstract void notUsed( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value );

        abstract void empty( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value );
    }
}
