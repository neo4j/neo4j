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

import org.neo4j.kernel.api.cursor.LabelCursor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Overlays transaction state on a {@link LabelCursor}.
 */
public class TxLabelCursor
    implements LabelCursor
{
    private KernelStatement statement;
    private InstanceCache<TxLabelCursor> instanceCache;
    private LabelCursor cursor;

    public TxLabelCursor( KernelStatement statement, InstanceCache<TxLabelCursor> instanceCache )
    {
        this.statement = statement;
        this.instanceCache = instanceCache;
    }

    public TxLabelCursor init( LabelCursor cursor )
    {
        this.cursor = cursor;
        return this;
    }

    @Override
    public boolean next()
    {
        return cursor.next();
    }

    @Override
    public boolean seek( int labelId )
    {
        return cursor.seek( labelId );
    }

    @Override
    public int getLabel()
    {
        return cursor.getLabel();
    }

    @Override
    public void close()
    {
        cursor.close();
        cursor = null;
        instanceCache.release( this );
    }
}
