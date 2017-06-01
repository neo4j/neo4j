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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;

public class NonUniqueNumberLayout extends NumberLayout
{
    private static final String IDENTIFIER_NAME = "NUNI";

    @Override
    public long identifier()
    {
        // todo Is Number.Value.SIZE a good checksum?
        return Layout.namedIdentifier( IDENTIFIER_NAME, NumberValue.SIZE );
    }

    @Override
    public int compare( NumberKey o1, NumberKey o2 )
    {
        int compare = Double.compare( o1.value, o2.value );
        return compare != 0 ? compare : Long.compare( o1.entityId, o2.entityId );
    }
}
