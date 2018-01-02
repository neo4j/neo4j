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
package org.neo4j.graphmatching;

import java.util.regex.Pattern;

/**
 * This class contain factory methods for some common {@link ValueMatcher}s.
 */
@Deprecated
public abstract class CommonValueMatchers
{
    private CommonValueMatchers()
    {
    }

    private static final ValueMatcher HAS = new ValueMatcher()
    {
        public boolean matches( Object value )
        {
            return value != null;
        }
    };

    /**
     * Checks for equality between a value and {@code valueToMatch}. Returns
     * {@code true} if the value isn't null and is equal to
     * {@code valueToMatch}, else {@code false}.
     *
     * @param valueToMatch the expected value.
     * @return whether or not a value is equal to {@code valueToMatch}.
     */
    public static ValueMatcher exact( Object valueToMatch )
    {
        return new ExactMatcher( valueToMatch );
    }

    /**
     * Checks for equality between a value and {@code valueToMatch}.
     * If the value is an array each item in the array is matched against
     * {@code valueToMatch} and if any of those matches it's considered
     * a match.
     *
     * @param valueToMatch the expected value.
     * @return whether or not a value is equal to {@code valueToMatch}.
     */
    public static ValueMatcher exactAny( Object valueToMatch )
    {
        return new ExactAnyMatcher( valueToMatch );
    }

    /**
     * Checks for equality between a value and any one of
     * {@code anyOfTheseToMatch}. If the value is an array each item in
     * the array is matched against any one of {@code valueToMatch} and if
     * any of those matches it's considered a match.
     *
     * @param anyOfTheseToMatch the expected value.
     * @return whether or not a value is equal to any one of
     * {@code anyOfTheseToMatch}.
     */
    public static ValueMatcher exactAnyOf( Object... anyOfTheseToMatch )
    {
        return new ExactAnyMatcher( anyOfTheseToMatch );
    }

    /**
     * Checks that the property exists.
     *
     * @return a matcher that verifies that the property exists.
     */
    public static ValueMatcher has()
    {
        return HAS;
    }

    /**
     * Checks that the {@link String} property matches the specified regular
     * expression pattern.
     *
     * @param pattern the regular expression pattern to match the property with.
     * @return a matcher that verifies that the property matches the given
     *         regular expression.
     */
    public static ValueMatcher regex( Pattern pattern )
    {
        return new RegexMatcher( pattern );
    }

    private static class ExactMatcher implements ValueMatcher
    {
        private final Object valueToMatch;

        public ExactMatcher( Object valueToMatch )
        {
            this.valueToMatch = valueToMatch;
        }

        public boolean matches( Object value )
        {
            return value != null && this.valueToMatch.equals( value );
        }
    }

    private static class ExactAnyMatcher implements ValueMatcher
    {
        private final Object[] valuesToMatch;

        public ExactAnyMatcher( Object... valueToMatch )
        {
            this.valuesToMatch = valueToMatch;
        }

        public boolean matches( Object value )
        {
            if ( value != null )
            {
                if ( value.getClass().isArray() )
                {
                    for ( Object item : ArrayPropertyUtil.propertyValueToCollection( value ) )
                    {
                        if ( item != null && anyMatches( item ) )
                        {
                            return true;
                        }
                    }
                }
                else if ( anyMatches( value ) )
                {
                    return true;
                }
            }
            return false;
        }

        private boolean anyMatches( Object value )
        {
            for ( Object matchValue : valuesToMatch )
            {
                if ( value.equals( matchValue ) )
                {
                    return true;
                }
            }
            return false;
        }
    }

    private static class RegexMatcher implements ValueMatcher
    {
        private final Pattern pattern;

        public RegexMatcher( Pattern pattern )
        {
            this.pattern = pattern;
        }

        public boolean matches( Object value )
        {
            return value != null && pattern.matcher( value.toString() ).matches();
        }
    }
}
