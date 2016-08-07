/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.tools.txlog.checktypes;

import org.junit.Test;

import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static org.junit.Assert.assertTrue;

public class RelationshipGroupCheckTypeTest
{
    @Test
    public void inUseRecordEquality()
    {
        RelationshipGroupRecord record1 = new RelationshipGroupRecord( 1 );
        record1.initialize( true, 1, 2, 3, 4, 5, 6 );
        record1.setSecondaryUnitId( 42 );

        RelationshipGroupRecord record2 = record1.clone();

        RelationshipGroupCheckType check = new RelationshipGroupCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }

    @Test
    public void notInUseRecordEquality()
    {
        RelationshipGroupRecord record1 = new RelationshipGroupRecord( 1 );
        record1.initialize( false, 1, 2, 3, 4, 5, 6 );
        record1.setSecondaryUnitId( 42 );

        RelationshipGroupRecord record2 = new RelationshipGroupRecord( 1 );
        record1.initialize( false, 11, 22, 33, 44, 55, 66 );
        record2.setSecondaryUnitId( 24 );

        RelationshipGroupCheckType check = new RelationshipGroupCheckType();

        assertTrue( check.equal( record1, record2 ) );
    }
}
