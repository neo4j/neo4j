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
package org.neo4j.kernel.api.impl.schema.verification;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.function.Factory;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.verification.DuplicateCheckStrategy.BucketsDuplicateCheckStrategy;
import org.neo4j.kernel.api.impl.schema.verification.DuplicateCheckStrategy.MapDuplicateCheckStrategy;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;

import static java.lang.String.format;
import static org.neo4j.kernel.api.impl.schema.verification.DuplicateCheckStrategy.BucketsDuplicateCheckStrategy.BUCKET_STRATEGY_ENTRIES_THRESHOLD;

@RunWith( Parameterized.class )
public class DuplicateCheckStrategyTest
{

    @Parameterized.Parameters
    public static List<Factory<? extends DuplicateCheckStrategy>> duplicateCheckStrategies()
    {
        return Arrays.asList( () -> new MapDuplicateCheckStrategy( 1000 ),
                () -> new BucketsDuplicateCheckStrategy( randomNumberOfEntries() ) );
    }

    @Parameterized.Parameter
    public Factory<DuplicateCheckStrategy> duplicateCheckStrategyFactory;
    private DuplicateCheckStrategy checkStrategy;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws IllegalAccessException, InstantiationException
    {
        checkStrategy = duplicateCheckStrategyFactory.newInstance();
    }

    @Test
    public void checkStringSinglePropertyDuplicates() throws Exception
    {
        String duplicatedString = "duplicate";
        DefinedProperty property = Property.property( 1, duplicatedString );

        expectedException.expect( IndexEntryConflictException.class );
        expectedException.expectMessage(
                format( "Both node 1 and node 2 share the property value ( '%s' )", duplicatedString ) );

        checkStrategy.checkForDuplicate( property, duplicatedString, 1 );
        checkStrategy.checkForDuplicate( property, duplicatedString, 2 );
    }

    @Test
    public void checkNumericSinglePropertyDuplicates() throws Exception
    {
        Number duplicatedNumber = 0.33d;
        DefinedProperty property = Property.property( 1, duplicatedNumber );

        expectedException.expect( IndexEntryConflictException.class );
        expectedException.expectMessage( format( "Both node 3 and node 4 share the property value ( %.2f )", duplicatedNumber.floatValue()) );

        checkStrategy.checkForDuplicate( property, duplicatedNumber, 3 );
        checkStrategy.checkForDuplicate( property, duplicatedNumber, 4 );
    }

    @Test
    public void duplicateFoundAmongUniqueStringSingleProperty() throws IndexEntryConflictException
    {
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            String propertyValue = String.valueOf( i );
            DefinedProperty property = Property.property( 1, propertyValue );
            checkStrategy.checkForDuplicate( property, propertyValue, i );
        }

        int duplicateTarget = BUCKET_STRATEGY_ENTRIES_THRESHOLD - 2;
        expectedException.expect( IndexEntryConflictException.class );
        expectedException.expectMessage(
                format( "Both node %d and node 3 share the property value ( '%s' )", duplicateTarget, duplicateTarget ) );
        String duplicatedValue = String.valueOf( duplicateTarget );
        DefinedProperty property = Property.property( 1, duplicatedValue );
        checkStrategy.checkForDuplicate( property, duplicatedValue, 3 );
    }

    @Test
    public void duplicateFoundAmongUniqueNumberSingleProperty() throws IndexEntryConflictException
    {
        double propertyValue = 0;
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            DefinedProperty property = Property.property( 1, propertyValue );
            checkStrategy.checkForDuplicate( property, propertyValue, i );
            propertyValue += 1;
        }

        int duplicateTarget = BUCKET_STRATEGY_ENTRIES_THRESHOLD - 8;
        double duplicateValue = duplicateTarget;
        expectedException.expect( IndexEntryConflictException.class );
        expectedException.expectMessage(
                format( "Both node %d and node 3 share the property value ( %.1f )", duplicateTarget, duplicateValue ) );
        DefinedProperty property = Property.property( 1, duplicateValue );
        checkStrategy.checkForDuplicate( property, duplicateValue, 3 );
    }

    @Test
    public void noDuplicatesDetectedForUniqueStringSingleProperty() throws IndexEntryConflictException
    {
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            String propertyValue = String.valueOf( i );
            DefinedProperty property = Property.property( 1, propertyValue );
            checkStrategy.checkForDuplicate( property, propertyValue, i );
        }
    }

    @Test
    public void noDuplicatesDetectedForUniqueNumberSingleProperty() throws IndexEntryConflictException
    {
        double propertyValue = 0;
        int numberOfIterations = randomNumberOfEntries();
        for ( int i = 0; i < numberOfIterations; i++ )
        {
            propertyValue += 1d / numberOfIterations;
            DefinedProperty property = Property.property( 1, propertyValue );
            checkStrategy.checkForDuplicate( property, propertyValue, i );
        }
    }

    // multiple

    @Test
    public void checkStringMultiplePropertiesDuplicates() throws Exception
    {
        String duplicateA = "duplicateA";
        String duplicateB = "duplicateB";
        DefinedProperty propertyA = Property.property( 1, duplicateA );
        DefinedProperty propertyB = Property.property( 2, duplicateB );

        expectedException.expect( IndexEntryConflictException.class );
        expectedException.expectMessage( format( "Both node 1 and node 2 share the property value ( '%s', '%s' )",
                duplicateA, duplicateB ) );

        checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB}, new Object[]{duplicateA, duplicateB}, 1 );
        checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB}, new Object[]{duplicateA, duplicateB}, 2 );
    }

    @Test
    public void checkNumericMultiplePropertiesDuplicates() throws Exception
    {
        Number duplicatedNumberA = 0.33d;
        Number duplicatedNumberB = 2;
        DefinedProperty propertyA = Property.property( 1, duplicatedNumberA );
        DefinedProperty propertyB = Property.property( 2, duplicatedNumberB );

        expectedException.expect( IndexEntryConflictException.class );
        expectedException.expectMessage( format( "Both node 3 and node 4 share the property value ( %.2f, %d )",
                duplicatedNumberA.floatValue(), duplicatedNumberB.intValue() ) );

        checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB}, new Object[]{duplicatedNumberA, duplicatedNumberB}, 3 );
        checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB}, new Object[]{duplicatedNumberA, duplicatedNumberB}, 4 );
    }

    @Test
    public void duplicateFoundAmongUniqueStringMultipleProperties() throws IndexEntryConflictException
    {
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            String propertyValueA = String.valueOf( i );
            String propertyValueB = String.valueOf( -i );
            DefinedProperty propertyA = Property.property( 1, propertyValueA );
            DefinedProperty propertyB = Property.property( 2, propertyValueB );
            checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB},
                    new Object[]{propertyValueA, propertyValueB}, i );
        }

        int duplicateTarget = BUCKET_STRATEGY_ENTRIES_THRESHOLD - 2;
        String duplicatedValueA = String.valueOf( duplicateTarget );
        String duplicatedValueB = String.valueOf( -duplicateTarget );
        expectedException.expect( IndexEntryConflictException.class );
        expectedException.expectMessage( format( "Both node %d and node 3 share the property value ( '%s', '%s' )",
                        duplicateTarget, duplicatedValueA, duplicatedValueB ) );
        DefinedProperty propertyA = Property.property( 1, duplicatedValueA);
        DefinedProperty propertyB = Property.property( 2, duplicatedValueB);
        checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB},
                new Object[]{duplicatedValueA, duplicatedValueB}, 3 );
    }

    @Test
    public void duplicateFoundAmongUniqueNumberMultipleProperties() throws IndexEntryConflictException
    {
        double propertyValue = 0;
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            double propertyValueA = propertyValue;
            double propertyValueB = -propertyValue;
            DefinedProperty propertyA = Property.property( 1, propertyValueA );
            DefinedProperty propertyB = Property.property( 2, propertyValueB );
            checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB},
                    new Object[]{propertyValueA, propertyValueB}, i );
            propertyValue += 1;
        }

        int duplicateTarget = BUCKET_STRATEGY_ENTRIES_THRESHOLD - 8;
        double duplicateValueA = duplicateTarget;
        double duplicateValueB = -duplicateTarget;
        expectedException.expect( IndexEntryConflictException.class );
        expectedException.expectMessage( format( "Both node %d and node 3 share the property value ( %s, %s )",
                duplicateTarget, duplicateValueA, duplicateValueB ) );
        DefinedProperty propertyA = Property.property( 1, duplicateValueA );
        DefinedProperty propertyB = Property.property( 2, duplicateValueB );
        checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB}, new Object[]{duplicateValueA, duplicateValueB}, 3 );
    }

    @Test
    public void noDuplicatesDetectedForUniqueStringMultipleProperties() throws IndexEntryConflictException
    {
        for ( int i = 0; i < randomNumberOfEntries(); i++ )
        {
            String propertyValueA = String.valueOf( i );
            String propertyValueB = String.valueOf( -i );
            DefinedProperty propertyA = Property.property( 1, propertyValueA );
            DefinedProperty propertyB = Property.property( 2, propertyValueB );
            checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB},
                    new Object[]{propertyValueA, propertyValueB}, i );
        }
    }

    @Test
    public void noDuplicatesDetectedForUniqueNumberMultipleProperties() throws IndexEntryConflictException
    {
        double propertyValueA = 0;
        double propertyValueB = 0;
        int numberOfIterations = randomNumberOfEntries();
        for ( int i = 0; i < numberOfIterations; i++ )
        {
            propertyValueA += 1d / numberOfIterations;
            propertyValueB -= 1d / numberOfIterations;
            DefinedProperty propertyA = Property.property( 1, propertyValueA );
            DefinedProperty propertyB = Property.property( 2, propertyValueB );
            checkStrategy.checkForDuplicate( new Property[]{propertyA, propertyB}, new Object[]{propertyValueA, propertyValueB}, i );
        }
    }

    private static int randomNumberOfEntries()
    {
        return ThreadLocalRandom.current().nextInt( BUCKET_STRATEGY_ENTRIES_THRESHOLD, BUCKET_STRATEGY_ENTRIES_THRESHOLD << 1 );
    }

}
