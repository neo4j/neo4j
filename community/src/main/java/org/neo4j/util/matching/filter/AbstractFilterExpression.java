package org.neo4j.util.matching.filter;

public abstract class AbstractFilterExpression implements FilterExpression
{
    private final String label;
    private final String property;
    
    public AbstractFilterExpression( String label, String property )
    {
        this.label = label;
        this.property = property;
    }
    
    public String getLabel()
    {
        return this.label;
    }
    
    public String getProperty()
    {
        return this.property;
    }
}
