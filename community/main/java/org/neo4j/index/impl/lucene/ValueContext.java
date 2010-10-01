package org.neo4j.index.impl.lucene;

public class ValueContext
{
    final Object value;
    boolean indexNumeric;

    public ValueContext( Object value )
    {
        this.value = value;
    }
    
    public ValueContext indexNumeric()
    {
        if ( !( this.value instanceof Number ) )
        {
            throw new IllegalStateException( "Value should be a Number, is " + value +
                    " (" + value.getClass() + ")" );
        }
        this.indexNumeric = true;
        return this;
    }
    
    Object getCorrectValue()
    {
        return this.indexNumeric ? this.value : this.value.toString();
    }
    
    @Override
    public String toString()
    {
        return value.toString();
    }
}
