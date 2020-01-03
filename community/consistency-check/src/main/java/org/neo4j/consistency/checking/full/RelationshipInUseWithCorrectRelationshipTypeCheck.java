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
package org.neo4j.consistency.checking.full;

import java.util.Arrays;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.ComparativeRecordChecker;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.consistency.checking.full.NodeInUseWithCorrectLabelsCheck.sortAndDeduplicate;

public class RelationshipInUseWithCorrectRelationshipTypeCheck
        <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport.RelationshipInUseWithCorrectRelationshipTypeReport>
        implements ComparativeRecordChecker<RECORD,RelationshipRecord, REPORT>
{
    private final long[] indexRelationshipTypes;

    public RelationshipInUseWithCorrectRelationshipTypeCheck( long[] expectedEntityTokenIds )
    {
        this.indexRelationshipTypes = sortAndDeduplicate( expectedEntityTokenIds );
    }

    @Override
    public void checkReference( RECORD record, RelationshipRecord relationshipRecord, CheckerEngine<RECORD,REPORT> engine, RecordAccess records )
    {
        if ( relationshipRecord.inUse() )
        {
            // Relationship indexes are always semantically multi-token, which means that the relationship record just need to have one of the possible
            // relationship types mentioned by the index. Relationships can't have more than one type anyway.
            long type = relationshipRecord.getType();
            if ( Arrays.binarySearch( indexRelationshipTypes, type ) < 0 )
            {
                // The relationship did not have any of the relationship types mentioned by the index.
                for ( long indexRelationshipType : indexRelationshipTypes )
                {
                    engine.report().relationshipDoesNotHaveExpectedRelationshipType( relationshipRecord, indexRelationshipType );
                }
            }
        }
        else
        {
            engine.report().relationshipNotInUse( relationshipRecord );
        }
    }
}
