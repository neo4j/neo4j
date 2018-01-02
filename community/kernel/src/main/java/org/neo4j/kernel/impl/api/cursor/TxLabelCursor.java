/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.cursor.LabelItem;
import org.neo4j.kernel.impl.util.diffsets.ReadableDiffSets;

/**
 * Overlays transaction state on a {@link LabelItem} cursor.
 */
public class TxLabelCursor
        implements Cursor<LabelItem>, LabelItem
{
    private final Consumer<TxLabelCursor> instanceCache;

    protected Cursor<LabelItem> cursor;
    protected ReadableDiffSets<Integer> labelDiffSet;

    protected int label;
    private Iterator<Integer> added;

    public TxLabelCursor( Consumer<TxLabelCursor> instanceCache )
    {
        this.instanceCache = instanceCache;
    }

    public TxLabelCursor init( Cursor<LabelItem> cursor, ReadableDiffSets<Integer> labelDiffSet )
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
                label = cursor.get().getAsInt();
                if ( labelDiffSet.isRemoved( label ) )
                {
                    continue;
                }
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
            label = StatementConstants.NO_SUCH_LABEL;
            return false;
        }
    }

    @Override
    public LabelItem get()
    {
        if ( label == StatementConstants.NO_SUCH_LABEL )
        {
            throw new IllegalStateException();
        }

        return this;
    }

    @Override
    public int getAsInt()
    {
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
