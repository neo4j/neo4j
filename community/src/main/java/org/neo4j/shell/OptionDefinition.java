package org.neo4j.shell;

/**
 * Groups an {@link OptionValueType} and a description.
 */
public class OptionDefinition
{
    private OptionValueType type;
    private String description;
    
    /**
     * @param type the type for the option.
     * @param description the description of the option.
     */
    public OptionDefinition( OptionValueType type, String description )
    {
        this.type = type;
        this.description = description;
    }
    
    /**
     * @return the option value type.
     */
    public OptionValueType getType()
    {
        return this.type;
    }
    
    /**
     * @return the description.
     */
    public String getDescription()
    {
        return this.description;
    }
}
