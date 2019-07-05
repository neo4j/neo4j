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
package org.neo4j.kernel.api.impl.schema.verification;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.verification.DuplicateCheckStrategy.BucketsDuplicateCheckStrategy;
import org.neo4j.kernel.api.impl.schema.verification.DuplicateCheckStrategy.MapDuplicateCheckStrategy;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.kernel.api.impl.schema.verification.DuplicateCheckStrategy.BucketsDuplicateCheckStrategy.BUCKET_STRATEGY_ENTRIES_THRESHOLD;

class DuplicateCheckStrategyTest
{

    private static Stream<Arguments> duplicateCheckStrategies()
    {
        return Stream.of(
            arguments( new MapDuplicateCheckStrategy( 1000 ) ),
            arguments( new BucketsDuplicateCheckStrategy( randomNumberOfEntries() ) )
        );
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void checkStringSinglePropertyDuplicates( DuplicateCheckStrategy checkStrategy ) throws Exception
    {
        String duplicatedString = "duplicate";
        Value propertyValue = Values.stringValue( duplicatedString );

        var e = assertThrows( IndexEntryConflictException.class, () ->
        {
            checkStrategy.checkForDuplicate( propertyValue, 1 );
            checkStrategy.checkForDuplicate( propertyValue, 2 );
        } );
        assertEquals( format( "Both node 1 and node 2 share the property value %s", ValueTuple.of( propertyValue ) ), e.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void checkNumericSinglePropertyDuplicates( DuplicateCheckStrategy checkStrategy ) throws Exception
    {
        double duplicatedNumber = 0.33d;
        Value property = Values.doubleValue( duplicatedNumber );

        var e = assertThrows( IndexEntryConflictException.class, () ->
        {
            checkStrategy.checkForDuplicate( property, 3 );
            checkStrategy.checkForDuplicate( property, 4 );
        } );
        assertEquals( format( "Both node 3 and node 4 share the property value %s", ValueTuple.of( property ) ), e.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void duplicateFoundAmongUniqueStringSingleProperty( DuplicateCheckStrategy checkStrategy ) throws IndexEntryConflictException
    {
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            String propertyValue = String.valueOf( i );
            TextValue stringValue = Values.stringValue( propertyValue );
            checkStrategy.checkForDuplicate( stringValue, i );
        }

        int duplicateTarget = BUCKET_STRATEGY_ENTRIES_THRESHOLD - 2;
        String duplicate = String.valueOf( duplicateTarget );
        TextValue duplicatedValue = Values.stringValue( duplicate );
        var e = assertThrows( IndexEntryConflictException.class, () ->
            checkStrategy.checkForDuplicate( duplicatedValue, 3 ) );
        assertEquals( format( "Both node %d and node 3 share the property value %s", duplicateTarget, ValueTuple.of( duplicatedValue ) ), e.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void duplicateFoundAmongUniqueNumberSingleProperty( DuplicateCheckStrategy checkStrategy ) throws IndexEntryConflictException
    {
        double propertyValue = 0;
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            Value doubleValue = Values.doubleValue( propertyValue );
            checkStrategy.checkForDuplicate( doubleValue, i );
            propertyValue += 1;
        }

        int duplicateTarget = BUCKET_STRATEGY_ENTRIES_THRESHOLD - 8;
        double duplicateValue = duplicateTarget;
        Value duplicate = Values.doubleValue( duplicateValue );
        var e = assertThrows( IndexEntryConflictException.class, () ->
            checkStrategy.checkForDuplicate( duplicate, 3 ) );
        assertEquals( format( "Both node %d and node 3 share the property value %s", duplicateTarget, ValueTuple.of( duplicate ) ), e.getMessage() );

    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void noDuplicatesDetectedForUniqueStringSingleProperty( DuplicateCheckStrategy checkStrategy ) throws IndexEntryConflictException
    {
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            String propertyValue = String.valueOf( i );
            Value value = Values.stringValue( propertyValue );
            checkStrategy.checkForDuplicate( value, i );
        }
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void noDuplicatesDetectedForUniqueNumberSingleProperty( DuplicateCheckStrategy checkStrategy ) throws IndexEntryConflictException
    {
        double propertyValue = 0;
        int numberOfIterations = randomNumberOfEntries();
        for ( int i = 0; i < numberOfIterations; i++ )
        {
            propertyValue += 1d / numberOfIterations;
            Value value = Values.doubleValue( propertyValue );
            checkStrategy.checkForDuplicate( value, i );
        }
    }

    // multiple

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void checkStringMultiplePropertiesDuplicates( DuplicateCheckStrategy checkStrategy ) throws Exception
    {
        String duplicateA = "duplicateA";
        String duplicateB = "duplicateB";
        Value propertyA = Values.stringValue( duplicateA );
        Value propertyB = Values.stringValue( duplicateB );

        var e = assertThrows( IndexEntryConflictException.class, () ->
        {
            checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, 1 );
            checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, 2 );
        } );
        assertEquals( format( "Both node 1 and node 2 share the property value %s", ValueTuple.of( duplicateA, duplicateB ) ), e.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void checkNumericMultiplePropertiesDuplicates( DuplicateCheckStrategy checkStrategy ) throws Exception
    {
        Number duplicatedNumberA = 0.33d;
        Number duplicatedNumberB = 2;
        Value propertyA = Values.doubleValue( duplicatedNumberA.doubleValue() );
        Value propertyB = Values.intValue( duplicatedNumberB.intValue() );

        var e = assertThrows( IndexEntryConflictException.class, () ->
        {
            checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, 3 );
            checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, 4 );
        } );
        assertEquals( format( "Both node 3 and node 4 share the property value %s",
            ValueTuple.of( propertyA, propertyB ) ), e.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void duplicateFoundAmongUniqueStringMultipleProperties( DuplicateCheckStrategy checkStrategy ) throws IndexEntryConflictException
    {
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            String propertyValueA = String.valueOf( i );
            String propertyValueB = String.valueOf( -i );
            Value propertyA = Values.stringValue( propertyValueA );
            Value propertyB = Values.stringValue( propertyValueB );
            checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, i );
        }

        int duplicateTarget = BUCKET_STRATEGY_ENTRIES_THRESHOLD - 2;
        String duplicatedValueA = String.valueOf( duplicateTarget );
        String duplicatedValueB = String.valueOf( -duplicateTarget );
        Value propertyA = Values.stringValue( duplicatedValueA );
        Value propertyB = Values.stringValue( duplicatedValueB );
        var e = assertThrows( IndexEntryConflictException.class, () ->
            checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, 3 ) );
        assertEquals( format( "Both node %d and node 3 share the property value %s",
            duplicateTarget, ValueTuple.of( propertyA, propertyB ) ), e.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void duplicateFoundAmongUniqueNumberMultipleProperties( DuplicateCheckStrategy checkStrategy ) throws IndexEntryConflictException
    {
        double propertyValue = 0;
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            double propertyValueA = propertyValue;
            double propertyValueB = -propertyValue;
            Value propertyA = Values.doubleValue( propertyValueA );
            Value propertyB = Values.doubleValue( propertyValueB );
            checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, i );
            propertyValue += 1;
        }

        int duplicateTarget = BUCKET_STRATEGY_ENTRIES_THRESHOLD - 8;
        double duplicateValueA = duplicateTarget;
        double duplicateValueB = -duplicateTarget;
        Value propertyA = Values.doubleValue( duplicateValueA );
        Value propertyB = Values.doubleValue( duplicateValueB );
        var e = assertThrows( IndexEntryConflictException.class, () -> checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, 3 ) );
        assertEquals( format( "Both node %d and node 3 share the property value %s",
            duplicateTarget, ValueTuple.of( duplicateValueA, duplicateValueB ) ), e.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void noDuplicatesDetectedForUniqueStringMultipleProperties( DuplicateCheckStrategy checkStrategy ) throws IndexEntryConflictException
    {
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            String propertyValueA = String.valueOf( i );
            String propertyValueB = String.valueOf( -i );
            Value propertyA = Values.stringValue( propertyValueA );
            Value propertyB = Values.stringValue( propertyValueB );
            checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, i );
        }
    }

    @ParameterizedTest
    @MethodSource( "duplicateCheckStrategies" )
    void noDuplicatesDetectedForUniqueNumberMultipleProperties( DuplicateCheckStrategy checkStrategy ) throws IndexEntryConflictException
    {
        double propertyValueA = 0;
        double propertyValueB = 0;
        int numberOfIterations = randomNumberOfEntries();
        for ( int i = 0; i < numberOfIterations; i++ )
        {
            propertyValueA += 1d / numberOfIterations;
            propertyValueB -= 1d / numberOfIterations;
            Value propertyA = Values.doubleValue( propertyValueA );
            Value propertyB = Values.doubleValue( propertyValueB );
            checkStrategy.checkForDuplicate( new Value[]{propertyA, propertyB}, i );
        }
    }

    private static int randomNumberOfEntries()
    {
        return ThreadLocalRandom.current().nextInt( BUCKET_STRATEGY_ENTRIES_THRESHOLD, BUCKET_STRATEGY_ENTRIES_THRESHOLD << 1 );
    }

}
