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
package org.neo4j.kernel.api.properties;

import org.junit.Test;

import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.properties.Property.doubleProperty;
import static org.neo4j.kernel.api.properties.Property.intProperty;
import static org.neo4j.kernel.api.properties.Property.lazyStringProperty;
import static org.neo4j.kernel.api.properties.Property.stringProperty;

public class DefinedPropertyTest
{
    @Test
    public void shouldSortStringPropertiesWithSameValueByPropertyKeyId()
    {
        DefinedProperty p1 = stringProperty( 1, "x" );
        DefinedProperty p2 = stringProperty( 2, "x" );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortStringPropertiesWithDifferentValueByPropertyKeyId()
    {
        DefinedProperty p1 = stringProperty( 1, "x" );
        DefinedProperty p2 = stringProperty( 2, "y" );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortStringPropertiesWithSamePropertyKeyIdByValue()
    {
        DefinedProperty p1 = stringProperty( 1, "x" );
        DefinedProperty p2 = stringProperty( 1, "y" );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortIntPropertiesWithSameValueByPropertyKeyId()
    {
        DefinedProperty p1 = intProperty( 1, 10 );
        DefinedProperty p2 = intProperty( 2, 10 );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortIntPropertiesWithDifferentValueByPropertyKeyId()
    {
        DefinedProperty p1 = intProperty( 1, 10 );
        DefinedProperty p2 = intProperty( 2, 20 );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortIntPropertiesWithSamePropertyKeyIdByValue()
    {
        DefinedProperty p1 = intProperty( 1, 10 );
        DefinedProperty p2 = intProperty( 1, 20 );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortDoublePropertiesWithSameValueByPropertyKeyId()
    {
        DefinedProperty p1 = doubleProperty( 1, 10 );
        DefinedProperty p2 = doubleProperty( 2, 10 );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortDoublePropertiesWithDifferentValueByPropertyKeyId()
    {
        DefinedProperty p1 = doubleProperty( 1, 10 );
        DefinedProperty p2 = doubleProperty( 2, 20 );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortDoublePropertiesWithSamePropertyKeyIdByValue()
    {
        DefinedProperty p1 = doubleProperty( 1, 10 );
        DefinedProperty p2 = doubleProperty( 1, 20 );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortIntAndDoublePropertiesWithSamePropertyKeyIdByValue()
    {
        DefinedProperty p1 = intProperty( 1, 10 );
        DefinedProperty p2 = doubleProperty( 1, 20 );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldSortDoubleAndStringPropertiesWithSamePropertyKeyIdByValue()
    {
        DefinedProperty p1 = doubleProperty( 1, 10 );
        DefinedProperty p2 = stringProperty( 1, "20" );

        assertThat( compare( p1, p2 ), greaterThan( 0 ) );
    }

    @Test
    public void shouldSortStringAndIntPropertiesWithSamePropertyKeyIdByValue()
    {
        DefinedProperty p1 = stringProperty( 1, "10" );
        DefinedProperty p2 = intProperty( 1, 20 );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    @Test
    public void shouldFindIntAndDoubleEqualForPropertiesWithSamePropertyKeyIdAndValue()
    {
        DefinedProperty p1 = intProperty( 1, 10 );
        DefinedProperty p2 = doubleProperty( 1, 10 );

        assertThat( compare( p1, p2 ), equalTo( 0 ) );
    }

    @Test
    public void shouldSortLazyStringAndNonLazyStringPropertiesByValue()
    {
        DefinedProperty p1 = lazyStringProperty( 1, new Callable<String>(){
            @Override
            public String call() throws Exception
            {
                return "x";
            }
        } );
        DefinedProperty p2 = stringProperty( 1, "y" );

        assertThat( compare( p1, p2 ), lessThan( 0 ) );
    }

    private int compare( DefinedProperty leftProperty, DefinedProperty rightProperty )
    {
        int leftComparison = DefinedProperty.COMPARATOR.compare( leftProperty, rightProperty );
        int rightComparison = DefinedProperty.COMPARATOR.compare( rightProperty, leftProperty );
        assertThat( sign ( leftComparison ) , equalTo( -sign( rightComparison ) ) );
        return leftComparison;
    }

    private int sign( int value )
    {
        return value == 0 ? 0 : ( value < 0 ? -1 : + 1 );
    }
}
