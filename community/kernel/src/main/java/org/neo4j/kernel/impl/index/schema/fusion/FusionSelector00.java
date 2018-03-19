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

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.TEMPORAL;

/**
 * Selector for index provider "lucene-1.x".
 * The version name "00" comes from lucene-1.x originally not being a fusion index.
 */
public class FusionSelector00 implements FusionIndexProvider.Selector
{
    @Override
    public void validateSatisfied( Object[] instances )
    {
        FusionIndexBase.validateSelectorInstances( instances, LUCENE, SPATIAL, TEMPORAL );
    }

    @Override
    public int selectSlot( Value... values )
    {
        if ( values.length > 1 )
        {
            // Multiple values must be handled by lucene
            return LUCENE;
        }

        Value singleValue = values[0];
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

    @Override
    public IndexReader select( IndexReader[] instances, IndexQuery... predicates )
    {
        if ( predicates.length > 1 )
        {
            return instances[LUCENE];
        }
        IndexQuery predicate = predicates[0];

        if ( predicate instanceof IndexQuery.ExactPredicate )
        {
            IndexQuery.ExactPredicate exactPredicate = (IndexQuery.ExactPredicate) predicate;
            return select( instances, exactPredicate.value() );
        }

        if ( predicate instanceof IndexQuery.RangePredicate )
        {
            switch ( predicate.valueGroup() )
            {
            case GEOMETRY:
                return instances[SPATIAL];
            case DATE:
            case LOCAL_DATE_TIME:
            case ZONED_DATE_TIME:
            case LOCAL_TIME:
            case ZONED_TIME:
            case DURATION:
                return instances[TEMPORAL];
            default: // fall through
            }
        }
        if ( predicate instanceof IndexQuery.ExistsPredicate )
        {
            return null;
        }
        return instances[LUCENE];
    }
}
