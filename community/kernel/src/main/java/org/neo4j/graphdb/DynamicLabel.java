package org.neo4j.graphdb;

/**
 * A dynamically instantiated and named {@link Label}. This class is
 * a convenience implementation of <code>Label</code> that is
 * typically used when labels are created and named after a
 * condition that can only be detected at runtime.
 *
 * @see Label
 */
public class DynamicLabel implements Label
{
    public static Label label( String labelName )
    {
        return new DynamicLabel( labelName );
    }

    private final String name;

    private DynamicLabel( String labelName )
    {
        this.name = labelName;
    }

    @Override
    public String name()
    {
        return this.name;
    }

    @Override
    public String toString()
    {
        return this.name;
    }
}
