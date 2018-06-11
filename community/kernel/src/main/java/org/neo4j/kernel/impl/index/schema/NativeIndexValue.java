/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.values.storable.Value;

/**
 * Value in a {@link GBPTree} handling numbers suitable for schema indexing.
 *
 * NOTE:  For the time being no data exists in {@link NativeIndexValue}, but since the layout is under development
 * it's very convenient to have this class still exist so that it's very easy to try out different types
 * of layouts without changing the entire stack of arguments. In the end it may just be that this class
 * will be deleted, but for now it sticks around.
 */
class NativeIndexValue
{
    static final int SIZE = 0;

    static final NativeIndexValue INSTANCE = new NativeIndexValue();

    void from( Value... values )
    {
        // not needed a.t.m.
    }

    @Override
    public String toString()
    {
        return "[no value]";
    }
}
