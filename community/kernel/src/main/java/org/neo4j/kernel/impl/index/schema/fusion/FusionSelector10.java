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
import org.neo4j.internal.kernel.api.IndexQuery.ExistsPredicate;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.internal.kernel.api.IndexQuery.ExactPredicate;
import static org.neo4j.internal.kernel.api.IndexQuery.NumberRangePredicate;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.NUMBER;


/**
 * Selector for "lucene+native-1.x".
 * Separates numbers into native index.
 */
public class FusionSelector10 implements FusionIndexProvider.Selector
{
    @Override
    public void validateSatisfied( Object[] instances )
    {
        FusionIndexBase.validateSelectorInstances( instances, NUMBER, LUCENE );
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

        if ( singleValue.valueGroup() == ValueGroup.NUMBER )
        {
            return NUMBER;
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

        if ( predicate instanceof ExactPredicate )
        {
            ExactPredicate exactPredicate = (ExactPredicate) predicate;
            return select( instances, exactPredicate.value() );
        }

        if ( predicate instanceof NumberRangePredicate )
        {
            return instances[NUMBER];
        }

        if ( predicate instanceof ExistsPredicate )
        {
            return null;
        }

        return instances[LUCENE];
    }
}
