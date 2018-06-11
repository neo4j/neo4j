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

import org.neo4j.index.internal.gbptree.Layout;

/**
 * {@link Layout} for numbers where numbers need to be unique.
 */
class NumberLayoutUnique extends NumberLayout
{
    private static final String IDENTIFIER_NAME = "UNI";
    private static final int MAJOR_VERSION = 0;
    private static final int MINOR_VERSION = 1;

    NumberLayoutUnique()
    {
        super( Layout.namedIdentifier( IDENTIFIER_NAME, NumberIndexKey.SIZE ), MAJOR_VERSION, MINOR_VERSION );
    }
}
