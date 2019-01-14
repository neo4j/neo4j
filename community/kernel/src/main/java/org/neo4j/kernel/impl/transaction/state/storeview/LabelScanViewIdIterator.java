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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Node id iterator used during index population when we go over node ids indexed in label scan store.
 */
class LabelScanViewIdIterator implements PrimitiveLongResourceIterator
{
    private LabelScanReader labelScanReader;
    private PrimitiveLongIterator idIterator;

    LabelScanViewIdIterator( LabelScanReader labelScanReader, int[] labelIds )
    {
        this.labelScanReader = labelScanReader;
        this.idIterator = labelScanReader.nodesWithAnyOfLabels( labelIds );
    }

    @Override
    public void close()
    {
        labelScanReader.close();
    }

    @Override
    public boolean hasNext()
    {
        return idIterator.hasNext();
    }

    @Override
    public long next()
    {
        return idIterator.next();
    }
}
