package org.neo4j.graphmatching.filter;

import java.util.regex.Pattern;

import org.neo4j.graphmatching.PatternNode;

/**
 * A regex pattern with or without options, f.ex. "i" means case-insensitive.
 */
public class RegexPattern extends AbstractFilterExpression
{
    private final Pattern pattern;
    
    /**
     * Constructs a new regex pattern for filtering.
     * 
     * @param label the {@link PatternNode} label.
     * @param property the property key to filter in.
     * @param pattern the pattern which the value should match.
     * @param options options for regex matching.
     */
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
