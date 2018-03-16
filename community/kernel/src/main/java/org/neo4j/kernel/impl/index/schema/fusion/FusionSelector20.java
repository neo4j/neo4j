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
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.TEMPORAL;

/**
 * Selector for "lucene+native-2.x".
 * Separates strings, numbers, temporal and spatial into native index.
 */
public class FusionSelector20 implements FusionIndexProvider.Selector
{
    @Override
    public void validateSatisfied( Object[] instances )
    {
        FusionIndexBase.validateSelectorInstances( instances, STRING, NUMBER, SPATIAL, TEMPORAL, LUCENE );
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
        if ( singleValue.valueGroup() == ValueGroup.TEXT )
        {
            return STRING;
        }

        if ( singleValue.valueGroup() == ValueGroup.NUMBER )
        {
            return NUMBER;
        }

        if ( Values.isGeometryValue( singleValue ) )
        {
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

        if ( predicate instanceof IndexQuery.StringPredicate )
        {
            return instances[STRING];
        }

        if ( predicate instanceof IndexQuery.RangePredicate )
        {
            switch ( predicate.valueGroup() )
            {
            case NUMBER:
                return instances[NUMBER];
            case GEOMETRY:
                return instances[SPATIAL];
            case TEXT:
                return instances[STRING];
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
        throw new UnsupportedOperationException( "This selector does not have support for predicate " + predicate );
    }
}
