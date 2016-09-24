/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index.btree;

import org.neo4j.io.pagecache.PageCursor;

public class LabelScanLayout implements TreeItemLayout<LabelScanKey,Void>
{
    private static final int KEY_SIZE = Integer.BYTES + Long.BYTES; // TODO could be 6B long instead

    @Override
    public int compare( LabelScanKey o1, LabelScanKey o2 )
    {
        int labelComparison = Integer.compare( o1.labelId, o2.labelId );
        return labelComparison != 0 ? labelComparison : Long.compare( o1.nodeId, o2.nodeId );
    }

    @Override
    public LabelScanKey newKey()
    {
        return new LabelScanKey();
    }

    @Override
    public Void newValue()
    {   // Intentionally null since this layout has no values
        return null;
    }

    @Override
    public int keySize()
    {
        return KEY_SIZE;
    }

    @Override
    public int valueSize()
    {   // Intentionally 0 since this layout has no values
        return 0;
    }

    @Override
    public void writeKey( PageCursor cursor, LabelScanKey key )
    {
        cursor.putInt( key.labelId );
        cursor.putLong( key.nodeId );
    }

    @Override
    public void writeValue( PageCursor cursor, Void value )
    {   // Intentionally left empty since this layout has no values
    }

    @Override
    public void readKey( PageCursor cursor, LabelScanKey into )
    {
        into.labelId = cursor.getInt();
        into.nodeId = cursor.getLong();
    }

    @Override
    public void readValue( PageCursor cursor, Void into )
    {   // Intentionally left empty since this layout has no values
    }
}
