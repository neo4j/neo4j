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

import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class NativeSelector implements FusionSchemaIndexProvider.Selector
{
    @Override
    public <T> T select( T nativeInstance, T luceneInstance, Value... values )
    {
        if ( values.length > 1 )
        {
            // Multiple values must be handled by lucene
            return luceneInstance;
        }

        Value singleValue = values[0];
        if ( singleValue.valueGroup() == ValueGroup.NUMBER )
        {
            // It's a number, the native can handle this
            return nativeInstance;
        }
        return luceneInstance;
    }
}
