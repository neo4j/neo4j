/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.function.Function;

import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.values.storable.ValueGroup;


/**
 * Selector for "lucene+native-2.x".
 * Separates strings, numbers, temporal and spatial into native index.
 */
public class FusionSlotSelector20 implements SlotSelector
{
    @Override
    public void validateSatisfied( IndexProvider[] instances )
    {
        SlotSelector.validateSelectorInstances( instances, STRING, NUMBER, SPATIAL, TEMPORAL, LUCENE );
    }

    @Override
    public <V> int selectSlot( V[] values, Function<V,ValueGroup> groupOf )
    {
        if ( values.length > 1 )
        {
            return LUCENE;
        }

        ValueGroup singleGroup = groupOf.apply( values[0] );
        switch ( singleGroup.category() )
        {
        case NUMBER:
            return NUMBER;
        case TEXT:
            return STRING;
        case GEOMETRY:
            return SPATIAL;
        case TEMPORAL:
            return TEMPORAL;
        case UNKNOWN:
            return UNKNOWN;
        default:
            return LUCENE;
        }
    }
}
