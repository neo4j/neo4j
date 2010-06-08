package org.neo4j.graphmatching.filter;

import org.neo4j.graphmatching.PatternElement;
import org.neo4j.graphmatching.PatternNode;

/**
 * Abstract class which contains {@link PatternElement} label and property key.
 */
public abstract class AbstractFilterExpression implements FilterExpression
{
    private final String label;
    private final String property;
    
    /**
     * Constructs a new filter expression.
     * @param label the {@link PatternNode} label.
     * @param property the property key.
     */
    public AbstractFilterExpression( String label, String property )
    {
        this.label = label;
        this.property = property;
    }
    
    /**
     * @return The {@link PatternNode} label.
     */
    public String getLabel()
    {
        return this.label;
    }
    
    /**
     * @return the property key.
     */
    public String getProperty()
    {
        return this.property;
    }
}
