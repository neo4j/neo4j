/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.values.storable.Value;

public interface IndexCursorProgressor
{
    boolean next();

    void close();

    interface NodeValueCursor
    {
        void initialize( IndexCursorProgressor progressor, int[] keys );

        boolean node( long reference, Value[] values );

        void done();
    }

    interface NodeLabelCursor
    {
        void initialize( IndexCursorProgressor progressor, boolean providesLabels );

        boolean node( long reference, LabelSet labels );

        void done();
    }

    interface ExplicitCursor
    {
        void initialize( IndexCursorProgressor progressor, int expectedSize );

        boolean entity( long reference, float score );

        void done();
    }
//
//    interface RelationshipManualCursor
//    {
//        void initialize( IndexCursorProgressor progressor, int expectedSize );
//
//        boolean node( long reference, float score, long source, int type, long target );
//
//        void done();
//    }
}
