/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.labelscan;

import java.util.Iterator;

import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;
import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;

public interface LabelScanReader
{
    PrimitiveLongIterator nodesWithLabel( int labelId );

    Iterator<Long> labelsForNode( long nodeId );

    void close();

    LabelScanReader EMPTY = new LabelScanReader()
    {
        @Override
        public PrimitiveLongIterator nodesWithLabel( int labelId )
        {
            return emptyPrimitiveLongIterator();
        }

        @Override
        public Iterator<Long> labelsForNode( long nodeId )
        {
            return emptyIterator();
        }

        @Override
        public void close()
        {   // Nothing to close
        }
    };
}
