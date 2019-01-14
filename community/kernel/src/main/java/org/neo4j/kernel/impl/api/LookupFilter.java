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
package org.neo4j.kernel.impl.api;

import java.util.Arrays;
import java.util.function.LongPredicate;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

/**
 * When looking up nodes by a property value, we have to do a two-stage check.
 * The first stage is to look up the value in lucene, that will find us nodes that may have
 * the correct property value.
 * Then the second stage is to ensure the values actually match the value we are looking for,
 * which requires us to load the actual property value and filter the result we got in the first stage.
 * <p>This class defines the methods for the second stage check.<p>
 */
public class LookupFilter
{
    private LookupFilter()
    {
    }

    /**
     * used by the consistency checker
     */
    public static PrimitiveLongIterator exactIndexMatches( PropertyAccessor accessor,
            PrimitiveLongIterator indexedNodeIds, IndexQuery... predicates )
    {
        if ( !indexedNodeIds.hasNext() )
        {
            return indexedNodeIds;
        }

        IndexQuery[] filteredPredicates =
                Arrays.stream( predicates )
                        .filter( LookupFilter::isNumericOrGeometricPredicate )
                        .toArray( IndexQuery[]::new );

        if ( filteredPredicates.length > 0 )
        {
            LongPredicate combinedPredicate = nodeId ->
            {
                try
                {
                    for ( IndexQuery predicate : filteredPredicates )
                    {
                        int propertyKeyId = predicate.propertyKeyId();
                        Value value = accessor.getPropertyValue( nodeId, propertyKeyId );
                        if ( !predicate.acceptsValue( value ) )
                        {
                            return false;
                        }
                    }
                    return true;
                }
                catch ( EntityNotFoundException ignored )
                {
                    return false; // The node has been deleted but was still reported from the index. CC will catch
                                  // this through other mechanism (NodeInUseWithCorrectLabelsCheck), so we can
                                  // silently ignore here
                }
            };
            return PrimitiveLongCollections.filter( indexedNodeIds, combinedPredicate );
        }
        return indexedNodeIds;
    }

    private static boolean isNumericOrGeometricPredicate( IndexQuery predicate )
    {

        if ( predicate.type() == IndexQuery.IndexQueryType.exact )
        {
            IndexQuery.ExactPredicate exactPredicate = (IndexQuery.ExactPredicate) predicate;
            if ( isNumberGeometryOrArray( exactPredicate.value() ) )
            {
                return true;
            }
        }
        else if ( predicate.type() == IndexQuery.IndexQueryType.range &&
                  ( predicate.valueGroup() == ValueGroup.NUMBER || predicate.valueGroup() == ValueGroup.GEOMETRY ) )
        {
            return true;
        }
        return false;
    }

    private static boolean isNumberGeometryOrArray( Value value )
    {
        return Values.isNumberValue( value ) || Values.isGeometryValue( value ) || Values.isArrayValue( value );
    }
}
