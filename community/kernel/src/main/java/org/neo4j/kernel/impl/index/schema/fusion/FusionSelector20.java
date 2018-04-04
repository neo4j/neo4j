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
            return LUCENE;
        }

        Value singleValue = values[0];
        switch ( singleValue.valueGroup().category() )
        {
        case NUMBER:
            return NUMBER;
        case TEXT:
            return STRING;
        case GEOMETRY:
            return SPATIAL;
        case TEMPORAL:
            return TEMPORAL;
        default:
            return LUCENE;
        }
    }

    @Override
    public IndexReader select( IndexReader[] instances, IndexQuery... predicates )
    {
        if ( predicates.length > 1 )
        {
            return instances[LUCENE];
        }

        IndexQuery predicate = predicates[0];
        switch ( predicate.valueGroup().category() )
        {
        case NUMBER:
            return instances[NUMBER];
        case TEXT:
            return instances[STRING];
        case GEOMETRY:
            return instances[SPATIAL];
        case TEMPORAL:
            return instances[TEMPORAL];
        case UNKNOWN:
            return null;
        default:
            return instances[LUCENE];
        }
    }
}
