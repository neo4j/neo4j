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
package org.neo4j.consistency.checking;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

// TODO: it would be great if this also checked for cyclic chains. (we would also need cycle checking for full check, and for relationships)
public enum OwnerChain
        implements ComparativeRecordChecker<PropertyRecord, PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
{
    NEW
    {
        @Override
        RecordReference<PropertyRecord> property( RecordAccess records, long id )
        {
            return records.property( id );
        }

        @Override
        RecordReference<NodeRecord> node( RecordAccess records, long id )
        {
            return records.node( id );
        }

        @Override
        RecordReference<RelationshipRecord> relationship( RecordAccess records, long id )
        {
            return records.relationship( id );
        }

        @Override
        RecordReference<NeoStoreRecord> graph( RecordAccess records )
        {
            return records.graph();
        }

        @Override
        void wrongOwner( ConsistencyReport.PropertyConsistencyReport report )
        {
            report.ownerDoesNotReferenceBack();
        }
    };

    private final ComparativeRecordChecker<PropertyRecord, PrimitiveRecord, ConsistencyReport.PropertyConsistencyReport>
            OWNER_CHECK =
            new ComparativeRecordChecker<PropertyRecord, PrimitiveRecord, ConsistencyReport.PropertyConsistencyReport>()
            {
                @Override
                public void checkReference( PropertyRecord record, PrimitiveRecord owner,
                                            CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                            RecordAccess records )
                {
                    if ( !owner.inUse() && !record.inUse() )
                    {
                        return;
                    }
                    if ( !owner.inUse() || Record.NO_NEXT_PROPERTY.is( owner.getNextProp() ) )
                    {
                        wrongOwner( engine.report() );
                    }
                    else if ( owner.getNextProp() != record.getId() )
                    {
                        engine.comparativeCheck( property( records, owner.getNextProp() ),
                                                 OwnerChain.this );
                    }
                }
            };

    @Override
    public void checkReference( PropertyRecord record, PropertyRecord property,
                                CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                                RecordAccess records )
    {
        if ( record.getId() != property.getId() )
        {
            if ( !property.inUse() || Record.NO_NEXT_PROPERTY.is( property.getNextProp() ) )
            {
                wrongOwner( engine.report() );
            }
            else if ( property.getNextProp() != record.getId() )
            {
                engine.comparativeCheck( property( records, property.getNextProp() ), this );
            }
        }
    }

    public void check( PropertyRecord record,
                CheckerEngine<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> engine,
                RecordAccess records )
    {
        engine.comparativeCheck( ownerOf( record, records ), OWNER_CHECK );
    }

    private RecordReference<? extends PrimitiveRecord> ownerOf( PropertyRecord record, RecordAccess records )
    {
        if ( record.getNodeId() != -1 )
        {
            return node( records, record.getNodeId() );
        }
        else if ( record.getRelId() != -1 )
        {
            return relationship( records, record.getRelId() );
        }
        else
        {
            return graph( records );
        }
    }

    abstract RecordReference<PropertyRecord> property( RecordAccess records, long id );

    abstract RecordReference<NodeRecord> node( RecordAccess records, long id );

    abstract RecordReference<RelationshipRecord> relationship( RecordAccess records, long id );

    abstract RecordReference<NeoStoreRecord> graph( RecordAccess records );

    abstract void wrongOwner( ConsistencyReport.PropertyConsistencyReport report );
}
