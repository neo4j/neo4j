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

public interface IndexProgressor
{
    boolean next();

    void close();

    interface NodeValueClient
    {
        void initialize( IndexProgressor progressor, int[] keys );

        boolean acceptNode( long reference, Value... values );

        void done();
    }

    interface NodeLabelCursor
    {
        void initialize( IndexProgressor progressor, boolean providesLabels );

        boolean node( long reference, LabelSet labels );

        void done();
    }

    interface ExplicitCursor
    {
        void initialize( IndexProgressor progressor, int expectedSize );

        boolean entity( long reference, float score );

        void done();
    }

    IndexProgressor EMPTY = new IndexProgressor()
    {
        @Override
        public boolean next()
        {
            return false;
        }

        @Override
        public void close()
        {   // no-op
        }
    };
}
