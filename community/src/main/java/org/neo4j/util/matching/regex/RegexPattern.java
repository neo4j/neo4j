package org.neo4j.util.matching.regex;

import java.util.regex.Pattern;

/**
 * A regex pattern with or without options, f.ex. "i" means case-insensitive.
 */
public class RegexPattern implements RegexExpression
{
    private final String label;
    private final String propertyKey;
    private final String pattern;
    private final String options;
    
    public RegexPattern( String label, String propertyKey, String pattern,
        String options )
    {
        this.label = label;
        this.propertyKey = propertyKey;
        this.pattern = pattern;
        this.options = options;
    }

    public boolean matches( RegexValueGetter valueGetter )
    {
        String values[] = valueGetter.getValues( this.label );
        for ( String value : values )
        {
            boolean isCaseInsensitive = hasOption( 'i' );
            String thePattern = isCaseInsensitive ? this.pattern.toLowerCase() :
                this.pattern;
            String theValue = isCaseInsensitive ? value.toLowerCase() : value;
            boolean matches = Pattern.matches( thePattern, theValue );
            if ( matches )
            {
                return true;
            }
        }
        return false;
    }
    
    public String getLabel()
    {
        return this.label;
    }
    
    public String getPropertyKey()
    {
        return this.propertyKey;
    }
    
    private boolean hasOption( char option )
    {
        return this.options != null && this.options.indexOf( option ) > -1;
    }
}
