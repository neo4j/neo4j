/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.Lookup;

public abstract class PropertyPredicate
{
    public static final Lookup.Transformation<PropertyPredicate> TRANSFORMATION = PredicateFactory.INSTANCE;

    public abstract boolean matches( Property property );

    public static PropertyPredicate equalTo( final Object value ) { return new PropertyPredicate() {
        @Override public boolean matches( Property property ) { return property.valueEquals( value ); }
    }; }

    static PropertyPredicate startsWith( final String prefix ) { return new StringPredicate() {
        @Override public boolean matches( String value ) { return value.startsWith( prefix ); }
    }; }
    static PropertyPredicate endsWith( final String suffix ) { return new StringPredicate() {
        @Override public boolean matches( String value ) { return value.endsWith( suffix ); }
    }; }

    static NumberPredicate lessThan( final long number ) { return new NumberPredicate() {
        @Override boolean matches( long value )   { return value < number; }
        @Override boolean matches( double value ) { return value < number; }
    }; }
    static NumberPredicate lessThan( final double number ) { return new NumberPredicate() {
        @Override boolean matches( long value )   { return value < number; }
        @Override boolean matches( double value ) { return value < number; }
    }; }
    static NumberPredicate lessThanOrEqualTo( final long number ) { return new NumberPredicate() {
        @Override boolean matches( long value )   { return value <= number; }
        @Override boolean matches( double value ) { return value <= number; }
    }; }
    static NumberPredicate lessThanOrEqualTo( final double number ) { return new NumberPredicate() {
        @Override boolean matches( long value )   { return value <= number; }
        @Override boolean matches( double value ) { return value <= number; }
    }; }
    static NumberPredicate greaterThan( final long number ) { return new NumberPredicate() {
        @Override boolean matches( long value )   { return value > number; }
        @Override boolean matches( double value ) { return value > number; }
    }; }
    static NumberPredicate greaterThan( final double number ) { return new NumberPredicate() {
        @Override boolean matches( long value )   { return value > number; }
        @Override boolean matches( double value ) { return value > number; }
    }; }
    static NumberPredicate greaterThanOrEqualTo( final long number ) { return new NumberPredicate() {
        @Override boolean matches( long value )   { return value >= number; }
        @Override boolean matches( double value ) { return value >= number; }
    }; }
    static NumberPredicate greaterThanOrEqualTo( final double number ) { return new NumberPredicate() {
        @Override boolean matches( long value )   { return value >= number; }
        @Override boolean matches( double value ) { return value >= number; }
    }; }

    static PropertyPredicate not( final PropertyPredicate predicate ) { return new PropertyPredicate() {
        @Override public boolean matches( Property property ) { return !predicate.matches( property ); }
    }; }

    static NumberPredicate and( final NumberPredicate first, final NumberPredicate other ) { return new NumberPredicate() {
        @Override boolean matches( long value )   { return first.matches( value ) && other.matches( value ); }
        @Override boolean matches( double value ) { return first.matches( value ) && other.matches( value ); }
    }; }

    private PropertyPredicate()
    {
    }

    static abstract class StringPredicate extends PropertyPredicate
    {
        @Override
        public final boolean matches( Property property )
        {
            if ( property instanceof StringProperty )
            {
                return matches( ((StringProperty) property).value() );
            }
            else if ( property instanceof LazyStringProperty )
            {
                return matches( ((LazyStringProperty) property).value() );
            }
            return false;
        }

        abstract boolean matches( String value );
    }

    static abstract class NumberPredicate extends PropertyPredicate
    {
        @Override
        public final boolean matches( Property property )
        {
            if ( property instanceof IntegralNumberProperty )
            {
                return matches( ((IntegralNumberProperty) property).longValue() );
            }
            if ( property instanceof FloatingPointNumberProperty )
            {
                return matches( ((FloatingPointNumberProperty) property).doubleValue() );
            }
            return false;
        }

        abstract boolean matches( long value );

        abstract boolean matches( double value );
    }
}
