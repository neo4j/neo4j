/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

enum RelationshipReferenceEncoding
{
    /** No encoding */
    NONE( 0 ),

    /** @see #encodeGroup(long) */
    GROUP( 1 );

    private static final RelationshipReferenceEncoding[] ENCODINGS = RelationshipReferenceEncoding.values();

    final long id;
    final long bits;

    RelationshipReferenceEncoding( long id )
    {
        this.id = id;
        this.bits = id << 60;
    }

    static RelationshipReferenceEncoding parseEncoding( long reference )
    {
        if ( reference == NO_ID )
        {
            return NONE;
        }
        return ENCODINGS[encodingId( reference )];
    }

    private static int encodingId( long reference )
    {
        return (int)((reference & References.FLAG_MASK) >> 60);
    }

    /**
     * Encode a group id as a relationship reference.
     */
    static long encodeGroup( long groupId )
    {
        return groupId | GROUP.bits | References.FLAG_MARKER;
    }
}
