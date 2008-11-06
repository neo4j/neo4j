package org.neo4j.util.matching;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.util.matching.filter.FilterExpression;

public class PatternGroup
{
    private Collection<FilterExpression> regexExpression =
        new ArrayList<FilterExpression>();
    
    public void addFilter( FilterExpression regexRepression )
    {
        this.regexExpression.add( regexRepression );
    }
    
    public FilterExpression[] getFilters()
    {
        return this.regexExpression.toArray(
            new FilterExpression[ this.regexExpression.size() ] );
    }
}
