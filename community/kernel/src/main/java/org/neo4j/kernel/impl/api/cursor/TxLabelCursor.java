/*
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
package org.neo4j.kernel.impl.api.cursor;

import java.util.Iterator;

import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.cursor.LabelCursor;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;

/**
 * Overlays transaction state on a {@link LabelCursor}.
 */
public class TxLabelCursor
        implements LabelCursor
{
    private final Consumer<TxLabelCursor> instanceCache;

    private LabelCursor cursor;
    private DiffSets<Integer> labelDiffSet;

    private int label;
    private Iterator<Integer> added;

    public TxLabelCursor( Consumer<TxLabelCursor> instanceCache )
    {
        this.instanceCache = instanceCache;
    }

    public TxLabelCursor init( LabelCursor cursor, DiffSets<Integer> labelDiffSet )
    {
        this.cursor = cursor;
        this.labelDiffSet = labelDiffSet;
        this.added = null;
        return this;
    }

    @Override
    public boolean next()
    {
        if ( added == null )
        {
            while ( cursor != null && cursor.next() )
            {
                if ( labelDiffSet.isRemoved( cursor.getLabel() ) )
                {
                    continue;
                }

                label = cursor.getLabel();
                return true;
            }

            added = labelDiffSet.getAdded().iterator();
        }

        if ( added.hasNext() )
        {
            label = added.next();
            return true;
        }
        else
        {
            label = -1;
            return false;
        }
    }

    @Override
    public boolean seek( int labelId )
    {
        if ( labelDiffSet.isAdded( labelId ) )
        {
            label = labelId;
            return true;
        }

        if ( labelDiffSet.isRemoved( labelId ) )
        {
            label = -1;
            return false;
        }

        if ( cursor.seek( labelId ) )
        {
            label = labelId;
            return true;
        }
        else
        {
            label = -1;
            return false;
        }
    }

    @Override
    public int getLabel()
    {
        if ( label == -1 )
        {
            throw new IllegalStateException();
        }

        return label;
    }

    @Override
    public void close()
    {
        cursor.close();
        cursor = null;
        instanceCache.accept( this );
    }
}
