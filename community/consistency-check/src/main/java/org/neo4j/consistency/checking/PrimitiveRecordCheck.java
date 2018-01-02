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

import java.util.Arrays;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

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
    PrimitiveRecordCheck( RecordField<RECORD,REPORT>... fields )
    {
        this.fields = Arrays.copyOf( fields, fields.length );
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
    public ComparativeRecordChecker<RECORD,PrimitiveRecord,REPORT> ownerCheck()
    {
        return ownerCheck;
    }
}
