/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.Arrays;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsWriter;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.index.schema.GenericKey.BIGGEST_STATIC_SIZE;

/**
 * Validates Value[] tuples, whether or not they fit inside a {@link GBPTree} with a layout using {@link CompositeGenericKey}.
 * Most values won't even be serialized to {@link CompositeGenericKey}, values that fit well within the margin.
 */
class GenericIndexKeyValidator implements Validator<Value[]>
{
    private final int maxLength;
    private final Layout<GenericKey,NativeIndexValue> layout;
    private final IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings;
    private final int maxNumberOfCRSs;

    GenericIndexKeyValidator( int maxLength, Layout<GenericKey,NativeIndexValue> layout,
            IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings, int pageSize )
    {
        this.maxLength = maxLength;
        this.layout = layout;
        this.spaceFillingCurveSettings = spaceFillingCurveSettings;

        // Here's the thing: we want to prevent the number of coordinate reference systems in any given index to exceed
        // max allowed such that all no longer fits in the GB+Tree header, because that would brick the index as well as the db.
        // This limit is high, over a hundred, so practically this limit isn't hit in any "normal" use case.
        // There's also no single point on the commit path, before tx is committed AND is single-threaded so that number of settings
        // can be 100% verified. What we can do is to set this limit a bit below the actual limit and verify on that limit
        // in this validator, which is called concurrently by multiple committers.
        int actualMax = SpaceFillingCurveSettingsWriter.maxNumberOfSettings( pageSize );
        this.maxNumberOfCRSs = actualMax - actualMax / 10;
    }

    @Override
    public void validate( Value[] values )
    {
        int worstCaseSize = worstCaseLength( values );
        if ( worstCaseSize > maxLength )
        {
            int size = actualLength( values );
            if ( size > maxLength )
            {
                throw new IllegalArgumentException( format(
                        "Property value size:%d of %s is too large to index into this particular index. Please see index documentation for limitations.",
                        size, Arrays.toString( values ) ) );
            }
        }

        if ( spaceFillingCurveSettings.additionalValuesCouldExceed( values, maxNumberOfCRSs ) )
        {
            throw new UnsupportedOperationException(
                    format( "Adding %s would exceed number of crs settings limit %d for this index", Arrays.toString( values ), maxNumberOfCRSs ) );
        }
    }

    /**
     * A method for calculating some sort of worst-case length of a value tuple. This have to be a cheap call and can return false positives.
     * It exists to avoid serializing all value tuples into native keys, which can be expensive.
     *
     * @param values the value tuple to calculate some exaggerated worst-case size of.
     * @return the calculated worst-case size of the value tuple.
     */
    private static int worstCaseLength( Value[] values )
    {
        int length = Long.BYTES;
        for ( Value value : values )
        {
            // Add some generic overhead, slightly exaggerated
            length += Long.BYTES;
            // Add worst-case length of this value
            length += worstCaseLength( value );
        }
        return length;
    }

    private static int worstCaseLength( AnyValue value )
    {
        if ( value.isSequenceValue() )
        {
            SequenceValue sequenceValue = (SequenceValue) value;
            if ( sequenceValue instanceof TextArray )
            {
                TextArray textArray = (TextArray) sequenceValue;
                int length = 0;
                for ( int i = 0; i < textArray.length(); i++ )
                {
                    length += stringWorstCaseLength( textArray.stringValue( i ).length() );
                }
                return length;
            }
            return sequenceValue.length() * BIGGEST_STATIC_SIZE;
        }
        else
        {
            switch ( ((Value) value).valueGroup().category() )
            {
            case TEXT:
                // For text, which is very dynamic in its nature do a worst-case off of number of characters in it
                return stringWorstCaseLength( ((TextValue) value).length() );
            default:
                // For all else then use the biggest possible value for a non-dynamic, non-array value a state can occupy
                return BIGGEST_STATIC_SIZE;
            }
        }
    }

    private static int stringWorstCaseLength( int stringLength )
    {
        return GenericKey.SIZE_STRING_LENGTH + stringLength * 4;
    }

    private int actualLength( Value[] values )
    {
        GenericKey key = layout.newKey();
        key.initialize( 0 /*doesn't quite matter for size calculations, but an important method to call*/ );
        for ( int i = 0; i < values.length; i++ )
        {
            key.initFromValue( i, values[i], NativeIndexKey.Inclusion.NEUTRAL );
        }
        return key.size();
    }
}
