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
package org.neo4j.values;

import java.util.Comparator;
import java.util.Iterator;

import static org.neo4j.values.SequenceValue.IterationPreference.RANDOM_ACCESS;

/**
 * Values that represent sequences of values (such as Lists or Arrays) need to implement this interface.
 * Thus we can get an equality check that is based on the values (e.g. List.equals(ArrayValue) )
 * Values that implement this interface also need to overwrite isSequence() to return true!
 *
 * Note that even though SequenceValue extends Iterable iterating over the sequence using iterator() might not be the
 * most performant method. Branch using iterationPreference() in performance critical code paths.
 */
public interface SequenceValue extends Iterable<AnyValue>
{
    /**
     * The preferred way to iterate this sequence. Preferred in this case means the method which is expected to be
     * the most performant.
     */
    enum IterationPreference
    {
        RANDOM_ACCESS,
        ITERATION
    }

    int length();

    AnyValue value( int offset );

    @Override
    Iterator<AnyValue> iterator();

    IterationPreference iterationPreference();

    default boolean equals( SequenceValue other )
    {
        if ( other == null )
        {
            return false;
        }

        IterationPreference pref = iterationPreference();
        IterationPreference otherPref = other.iterationPreference();
        if ( pref == RANDOM_ACCESS && otherPref == RANDOM_ACCESS )
        {
            return equalsUsingRandomAccess( this, other );
        }
        else
        {
            return equalsUsingIterators( this, other );
        }
    }

    static boolean equalsUsingRandomAccess( SequenceValue a, SequenceValue b )
    {
        int i = 0;
        boolean areEqual = a.length() == b.length();

        while ( areEqual && i < a.length() )
        {
            areEqual = a.value( i ).equals( b.value( i ) );
            i++;
        }
        return areEqual;
    }

    static Boolean ternaryEqualsUsingRandomAccess( SequenceValue a, SequenceValue b )
    {
        if ( a.length() != b.length() )
        {
            return Boolean.FALSE;
        }

        int i = 0;
        Boolean equivalenceResult = Boolean.TRUE;

        while ( i < a.length() )
        {
            Boolean areEqual = a.value( i ).ternaryEquals( b.value( i ) );
            if ( areEqual == null )
            {
                equivalenceResult = null;
            }
            else if ( !areEqual )
            {
                return Boolean.FALSE;
            }
            i++;
        }

        return equivalenceResult;
    }

    static boolean equalsUsingIterators( SequenceValue a, SequenceValue b )
    {
        boolean areEqual = true;
        Iterator<AnyValue> aIterator = a.iterator();
        Iterator<AnyValue> bIterator = b.iterator();

        while ( areEqual && aIterator.hasNext() && bIterator.hasNext() )
        {
            areEqual = aIterator.next().equals( bIterator.next() );
        }

        return areEqual && aIterator.hasNext() == bIterator.hasNext();
    }

    static Boolean ternaryEqualsUsingIterators( SequenceValue a, SequenceValue b )
    {
        Boolean equivalenceResult = Boolean.TRUE;
        Iterator<AnyValue> aIterator = a.iterator();
        Iterator<AnyValue> bIterator = b.iterator();

        while ( aIterator.hasNext() && bIterator.hasNext() )
        {
            Boolean areEqual = aIterator.next().ternaryEquals( bIterator.next() );
            if ( areEqual == null )
            {
                equivalenceResult = null;
            }
            else if ( !areEqual )
            {
                return Boolean.FALSE;
            }
        }

        return aIterator.hasNext() == bIterator.hasNext() ? equivalenceResult : Boolean.FALSE;
    }

    default int compareToSequence( SequenceValue other, Comparator<AnyValue> comparator )
    {
        IterationPreference pref = iterationPreference();
        IterationPreference otherPref = other.iterationPreference();
        if ( pref == RANDOM_ACCESS && otherPref == RANDOM_ACCESS )
        {
            return compareUsingRandomAccess( this, other, comparator );
        }
        else
        {
            return compareUsingIterators( this, other, comparator );
        }
    }

    static int compareUsingRandomAccess( SequenceValue a, SequenceValue b, Comparator<AnyValue> comparator )
    {
        int i = 0;
        int x = 0;
        int length = Math.min( a.length(), b.length() );

        while ( x == 0 && i < length )
        {
            x = comparator.compare( a.value( i ), b.value( i ) );
            i++;
        }

        if ( x == 0 )
        {
            x = a.length() - b.length();
        }

        return x;
    }

    static int compareUsingIterators( SequenceValue a, SequenceValue b, Comparator<AnyValue> comparator )
    {
        int x = 0;
        Iterator<AnyValue> aIterator = a.iterator();
        Iterator<AnyValue> bIterator = b.iterator();

        while ( aIterator.hasNext() && bIterator.hasNext() )
        {
            x = comparator.compare( aIterator.next(), bIterator.next() );
        }

        if ( x == 0 )
        {
            x = Boolean.compare( aIterator.hasNext(), bIterator.hasNext() );
        }

        return x;
    }

    default Boolean ternaryEquality( SequenceValue other )
    {
        IterationPreference pref = iterationPreference();
        IterationPreference otherPref = other.iterationPreference();
        if ( pref == RANDOM_ACCESS && otherPref == RANDOM_ACCESS )
        {
            return ternaryEqualsUsingRandomAccess(this, other );
        }
        else
        {
            return ternaryEqualsUsingIterators( this, other );
        }
    }
}
