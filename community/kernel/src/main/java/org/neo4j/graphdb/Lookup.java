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
package org.neo4j.graphdb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static java.util.Objects.requireNonNull;

public abstract class Lookup
{
    public static Lookup equalTo( final Object value ) { requireNonNull( value, "value" ); return new Lookup() {
        @Override public <T> T transform( Transformation<T> as ) { return as.equalTo( value ); }
    }; }
    public static Lookup startsWith( final String prefix ) { requireNonNull( prefix, "prefix" ); return new Lookup() {
        @Override public <T> T transform( Transformation<T> as ) { return as.startsWith( prefix ); }
    }; }
    public static Lookup endsWith( final String suffix ) { requireNonNull( suffix, "suffix" ); return new Lookup() {
        @Override public <T> T transform( Transformation<T> as ) { return as.endsWith( suffix ); }
    }; }
//    public static Lookup matches( final String pattern ) { return new Lookup() {
//        @Override public <T> T transform( Transformation<T> as ) { return as.matches( pattern ); }
//    }; }
    public static UpperBound lessThan( final Number upper ) { requireNonNull( upper, "bound" ); return new UpperBound() {
        @Override public <T> T transform( Transformation<T> as ) { return as.lessThan( upper ); }
        @Override public Lookup andGreaterThan( final Number lower ) {
            requireNonNull( lower, "bound" ); return range( false, lower, upper, false ); }
        @Override public Lookup andGreaterThanOrEqualTo( final Number lower ) {
            requireNonNull( lower, "bound" ); return range( true, lower, upper, false ); }
        @Override Lookup negated() { return greaterThanOrEqualTo( upper ); }
    }; }
    public static UpperBound lessThanOrEqualTo( final Number upper ) { requireNonNull( upper, "bound" ); return new UpperBound() {
        @Override public <T> T transform( Transformation<T> as ) { return as.lessThanOrEqualTo( upper ); }
        @Override public Lookup andGreaterThan( final Number lower ) {
            requireNonNull( lower, "bound" ); return range( false, lower, upper, true ); }
        @Override public Lookup andGreaterThanOrEqualTo( final Number lower ) {
            requireNonNull( lower, "bound" ); return range( true, lower, upper, true ); }
        @Override Lookup negated() { return greaterThan( upper ); }
    }; }
    public static LowerBound greaterThan( final Number lower ) { requireNonNull( lower, "bound" ); return new LowerBound() {
        @Override public <T> T transform( Transformation<T> as ) { return as.greaterThan( lower ); }
        @Override public Lookup andLessThan( final Number upper ) {
            requireNonNull( upper, "bound" ); return range( false, lower, upper, false ); }
        @Override public Lookup andLessThanOrEqualTo( final Number upper ) {
            requireNonNull( upper, "bound" ); return range( false, lower, upper, true ); }
        @Override Lookup negated() { return lessThanOrEqualTo( lower ); }
    }; }
    public static LowerBound greaterThanOrEqualTo( final Number lower ) { requireNonNull( lower, "bound" ); return new LowerBound() {
        @Override public <T> T transform( Transformation<T> as ) { return as.greaterThanOrEqualTo( lower ); }
        @Override public Lookup andLessThan( final Number upper ) {
            requireNonNull( upper, "bound" ); return range( true, lower, upper, false ); }
        @Override public Lookup andLessThanOrEqualTo( final Number upper ) {
            requireNonNull( upper, "bound" ); return range( true, lower, upper, true ); }
        @Override Lookup negated() { return lessThan( lower ); }
    }; }
    public static Lookup not( final Lookup lookup ) { requireNonNull( lookup, "Lookup" ); return lookup.negated(); }

    public static abstract class UpperBound extends Lookup
    {
        public abstract Lookup andGreaterThan( final Number value );
        public abstract Lookup andGreaterThanOrEqualTo( final Number value );
        private UpperBound(){}
    }
    public static abstract class LowerBound extends Lookup
    {
        public abstract Lookup andLessThan( final Number value );
        public abstract Lookup andLessThanOrEqualTo( final Number value );
        private LowerBound(){}
    }

    public abstract <T> T transform( Transformation<T> as );

    public interface Transformation<T>
    {
        T equalTo( Object value );
        T startsWith( String prefix );
        T endsWith( String suffix );
        //T matches( String pattern );
        T lessThan( Number value );
        T greaterThan( Number value );
        T lessThanOrEqualTo( Number value );
        T greaterThanOrEqualTo( Number value );
        T range( boolean includeLower, Number lower, Number upper, boolean includeUpper );
        T not( T lookup );
    }

    private Lookup()
    {
        // only internal sub-classes
    }

    Lookup negated() { return new Lookup() {
        @Override public <T> T transform( Transformation<T> as ) { return as.not( Lookup.this.transform( as ) ); }
        @Override Lookup negated() { return Lookup.this; }
    }; }

    @Override public String toString() { return transform( TO_STRING ); }
    @Override public int hashCode() { return transform( PARAMETER ).hashCode(); }

    @Override
    public boolean equals( Object that )
    {
        if ( this == that )
        {
            return true;
        }
        if ( that != null && this.getClass() == that.getClass() )
        {
            return transform( PARAMETER ).equals( ((Lookup) that).transform( PARAMETER ) );
        }
        return false;
    }

    private static Lookup range( final boolean includeLower, final Number lower,
                                 final Number upper, final boolean includeUpper )
    {
        if ( (includeLower && includeUpper) && // equal (through the definition "a<=b && b<=a means a==b")
             orderly( upper, lower, true ) && orderly( lower, upper, true ) )
        {
            return equalTo( lower );
        }
        Range range = new Range( includeLower, lower, upper, includeUpper );
        if ( !orderly( lower, upper, includeLower && includeUpper ) )
        {
            throw new IllegalArgumentException( "Invalid range: " + range.toString() );
        }
        return range;
    }

    private static final class Range extends Lookup
    {
        private final boolean includeLower;
        private final Number lower;
        private final Number upper;
        private final boolean includeUpper;

        Range( boolean includeLower, Number lower, Number upper, boolean includeUpper )
        {
            this.includeLower = includeLower;
            this.lower = lower;
            this.upper = upper;
            this.includeUpper = includeUpper;
        }

        @Override
        public <T> T transform( Transformation<T> as )
        {
            return as.range( includeLower, lower, upper, includeUpper );
        }

        @Override
        public String toString()
        {
            return (includeLower ? "greaterThanOrEqualTo(" : "greaterThan(") + lower +
                   (includeUpper ? ").andLessThanOrEqualTo(" : ").andLessThan(") + upper + ")";
        }

        @Override
        public int hashCode()
        {
            return lower.hashCode() ^ upper.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj instanceof Range )
            {
                Range that = (Range) obj;
                return this.includeLower == that.includeLower &&
                       this.includeUpper == that.includeUpper &&
                       this.lower.equals( that.lower ) &&
                       this.upper.equals( that.upper );
            }
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static final Transformation<String> TO_STRING = (Transformation) Proxy.newProxyInstance(
        Lookup.class.getClassLoader(), new Class[]{Transformation.class}, new InvocationHandler() {
            @Override public Object invoke( Object proxy, Method method, Object[] args )
            {
                return method.getName() + "(" + args[0] + ")";
            } } );
    @SuppressWarnings("unchecked")
    private static final Transformation<Object> PARAMETER = (Transformation) Proxy.newProxyInstance(
            Lookup.class.getClassLoader(), new Class[]{Transformation.class}, new InvocationHandler()
            {
                @Override
                public Object invoke( Object proxy, Method method, Object[] args )
                {
                    return args[0];
                }
            } );

    private static boolean orderly( Number lo, Number hi, boolean inclusive )
    {
        if ( lo instanceof Double || lo instanceof Float )
        {
            if ( hi instanceof Double || hi instanceof Float )
            {
                return inclusive ? lo.doubleValue() <= hi.doubleValue() :lo.doubleValue() < hi.doubleValue();
            }
            else
            {
                return inclusive ? lo.doubleValue() <= hi.longValue() :lo.doubleValue() < hi.longValue();
            }
        }
        else
        {
            if ( hi instanceof Double || hi instanceof Float )
            {
                return inclusive ? lo.longValue() <= hi.doubleValue() :lo.longValue() < hi.doubleValue();
            }
            else
            {
                return inclusive ? lo.longValue() <= hi.longValue() :lo.longValue() < hi.longValue();
            }
        }
    }
}
