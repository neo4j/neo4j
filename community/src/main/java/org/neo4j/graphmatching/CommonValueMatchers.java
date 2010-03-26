package org.neo4j.graphmatching;

import java.util.regex.Pattern;

public abstract class CommonValueMatchers
{
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
    
    public static ValueMatcher has()
    {
        return HAS;
    }
    
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
        private final Object valueToMatch;

        public ExactAnyMatcher( Object valueToMatch )
        {
            this.valueToMatch = valueToMatch;
        }

        public boolean matches( Object value )
        {
            if ( value == null )
            {
                return false;
            }
            for ( Object item : ArrayPropertyUtil.propertyValueToCollection( value ) )
            {
                if ( item != null && item.equals( valueToMatch ) )
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
