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
 * Simple GBPTree Layout with no values.
 *
 * @param <KEY> the key type
 */
abstract class BaseLayout<KEY extends ComparableNativeSchemaKey<KEY>> extends SchemaLayout<KEY>
{
    BaseLayout( String layoutName, int majorVersion, int minorVersion )
    {
        super( Layout.namedIdentifier( layoutName, NativeSchemaValue.SIZE ), majorVersion, minorVersion );
    }

    @Override
    int compareValue( KEY o1, KEY o2 )
    {
        return o1.compareValueTo( o2 );
    }
}
