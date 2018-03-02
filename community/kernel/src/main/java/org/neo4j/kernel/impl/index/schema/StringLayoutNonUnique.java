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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;

/**
 * {@link Layout} for strings that doesn't need to be unique.
 */
public class StringLayoutNonUnique extends StringLayout
{
    private static final String IDENTIFIER_NAME = "NUSI";
    private static final int MAJOR_VERSION = 0;
    private static final int MINOR_VERSION = 1;
    private static long IDENTIFIER = Layout.namedIdentifier( IDENTIFIER_NAME, NativeSchemaValue.SIZE );

    @Override
    public long identifier()
    {
        return IDENTIFIER;
    }

    @Override
    public int majorVersion()
    {
        return MAJOR_VERSION;
    }

    @Override
    public int minorVersion()
    {
        return MINOR_VERSION;
    }

    @Override
    public int compare( StringSchemaKey o1, StringSchemaKey o2 )
    {
        int comparison = o1.compareValueTo( o2 );
        return comparison != 0 ? comparison : Long.compare( o1.getEntityId(), o2.getEntityId() );
    }
}
