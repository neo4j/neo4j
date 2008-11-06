package org.neo4j.util.matching;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.util.matching.regex.RegexExpression;

public class PatternGroup
{
    private Collection<RegexExpression> regexExpression =
        new ArrayList<RegexExpression>();
    
    public void addFilter( RegexExpression regexRepression )
    {
        this.regexExpression.add( regexRepression );
    }
    
    public RegexExpression[] getFilters()
    {
        return this.regexExpression.toArray(
            new RegexExpression[ this.regexExpression.size() ] );
    }
}
