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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.cursor.LabelItem;

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

/**
 * Cursor over all labels on a node.
 */
public class StoreLabelCursor implements Cursor<LabelItem>, LabelItem
{
    private long[] labels;
    private int index;
    private int currentLabel;
    private Consumer<StoreLabelCursor> instanceCache;

    public StoreLabelCursor( Consumer<StoreLabelCursor> instanceCache )
    {
        this.instanceCache = instanceCache;
    }

    public StoreLabelCursor init( long[] labels )
    {
        this.labels = labels;
        index = 0;
        return this;
    }

    @Override
    public boolean next()
    {
        if ( index < labels.length )
        {
            currentLabel = safeCastLongToInt( labels[index++] );
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public LabelItem get()
    {
        return this;
    }

    @Override
    public int getAsInt()
    {
        return currentLabel;
    }

    @Override
    public void close()
    {
        instanceCache.accept( this );
    }
}
