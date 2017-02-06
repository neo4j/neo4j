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
package org.neo4j.kernel.impl.api.cursor;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.storageengine.api.txstate.ReadableDiffSets;

/**
 * Overlays transaction state on a label cursor.
 */
public class TxLabelIterator extends PrimitiveIntCollections.PrimitiveIntBaseIterator
{
    private PrimitiveIntIterator iterator;
    private ReadableDiffSets<Integer> labelDiffSet;
    private Iterator<Integer> added;

    public TxLabelIterator( PrimitiveIntIterator iterator, ReadableDiffSets<Integer> labelDiffSet )
    {
        this.iterator = iterator;
        this.labelDiffSet = labelDiffSet;
        this.added = null;
    }

    @Override
    protected boolean fetchNext()
    {
        if ( added == null )
        {
            while ( iterator != null && iterator.hasNext() )
            {
                int label = iterator.next();
                if ( labelDiffSet.isRemoved( label ) )
                {
                    continue;
                }
                next( label );
                return true;
            }

            added = labelDiffSet.getAdded().iterator();
        }

        if ( added.hasNext() )
        {
            next( added.next() );
            return true;
        }

        return false;
    }
}
