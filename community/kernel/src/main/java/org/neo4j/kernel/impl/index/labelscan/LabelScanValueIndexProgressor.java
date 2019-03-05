/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index.labelscan;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.api.index.IndexProgressor;

/**
 * {@link IndexProgressor} over a set of nodes with a given set of of labels.
 */
public class LabelScanValueIndexProgressor implements IndexProgressor, Resource
{
    private final PrimitiveLongResourceIterator ids;
    private final NodeLabelClient client;

    public LabelScanValueIndexProgressor( PrimitiveLongResourceIterator ids, NodeLabelClient client )
    {
        this.ids = ids;
        this.client = client;
    }

    /**
     * Progress through the index until the next accepted entry.
     * @return <code>true</code> if an accepted entry was found, <code>false</code> otherwise
     */
    @Override
    public boolean next()
    {
        while ( ids.hasNext() )
        {
            long id = ids.next();
            if ( client.acceptNode( id, null ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close()
    {
        ids.close();
    }
}
