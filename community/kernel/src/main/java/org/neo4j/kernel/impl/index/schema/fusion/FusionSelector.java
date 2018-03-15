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

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.TEMPORAL;

public class FusionSelector implements FusionIndexProvider.Selector
{
    @Override
    public int selectSlot( Value... values )
    {
        if ( values.length > 1 )
        {
            // Multiple values must be handled by lucene
            return LUCENE;
        }

        Value singleValue = values[0];
        if ( singleValue.valueGroup() == ValueGroup.TEXT )
        {
            // It's a string, the native string index can handle this
            return STRING;
        }

        if ( singleValue.valueGroup() == ValueGroup.NUMBER )
        {
            // It's a number, the native index can handle this
            return NUMBER;
        }

        if ( Values.isGeometryValue( singleValue ) )
        {
            // It's a geometry, the spatial index can handle this
            return SPATIAL;
        }

        if ( Values.isTemporalValue( singleValue ) )
        {
            return TEMPORAL;
        }

        return LUCENE;
    }
}
