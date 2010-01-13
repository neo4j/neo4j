package org.neo4j.graphmatching.filter;

import java.util.regex.Pattern;

/**
 * A regex pattern with or without options, f.ex. "i" means case-insensitive.
 */
public class RegexPattern extends AbstractFilterExpression
{
    private final Pattern pattern;
    
    public RegexPattern( String label, String property, String pattern,
        String options )
    {
        super( label, property );
        int op = 0;
        op |= hasOption( options, 'i' ) ? Pattern.CASE_INSENSITIVE : 0;
        this.pattern = Pattern.compile( pattern, op );
    }

    public boolean matches( FilterValueGetter valueGetter )
    {
        Object values[] = valueGetter.getValues( getLabel() );
        for ( Object value : values )
        {
            boolean matches = this.pattern.matcher( value.toString() ).find();
            if ( matches )
            {
                return true;
            }
        }
        return false;
    }
    
    private static boolean hasOption( String options, char option )
    {
        return options != null && options.indexOf( option ) > -1;
    }
}
