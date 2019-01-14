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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

import org.neo4j.helpers.ArrayUtil;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.values.storable.ValueCategory;

import static java.lang.String.format;

class CapabilityValidator
{
    static void validateQuery( IndexCapability capability, IndexOrder indexOrder, IndexQuery[] predicates )
    {
        if ( indexOrder != IndexOrder.NONE )
        {
            ValueCategory valueCategory = predicates[0].valueGroup().category();
            IndexOrder[] orderCapability = capability.orderCapability( valueCategory );
            if ( !ArrayUtil.contains( orderCapability, indexOrder ) )
            {
                orderCapability = ArrayUtils.add( orderCapability, IndexOrder.NONE );
                throw new UnsupportedOperationException(
                        format( "Tried to query index with unsupported order %s. Supported orders for query %s are %s.", indexOrder,
                                Arrays.toString( predicates ), Arrays.toString( orderCapability ) ) );
            }
        }
    }
}
