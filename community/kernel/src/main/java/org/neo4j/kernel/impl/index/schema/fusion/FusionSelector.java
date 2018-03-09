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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

public class FusionSelector implements FusionIndexProvider.Selector
{
    @Override
    public <T> T select( T numberInstance, T spatialInstance, T temporalInstance, T luceneInstance, Value... values )
    {
        if ( values.length > 1 )
        {
            // Multiple values must be handled by lucene
            return luceneInstance;
        }

        Value singleValue = values[0];
        if ( singleValue.valueGroup() == ValueGroup.NUMBER )
        {
            // It's a number, the native index can handle this
            return numberInstance;
        }

        if ( Values.isGeometryValue( singleValue ) )
        {
            // It's a geometry, the spatial index can handle this
            return spatialInstance;
        }

        if ( Values.isTemporalValue( singleValue ) )
        {
            return temporalInstance;
        }
        return luceneInstance;
    }
}
