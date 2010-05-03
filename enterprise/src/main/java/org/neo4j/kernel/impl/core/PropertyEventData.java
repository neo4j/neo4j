package org.neo4j.kernel.impl.core;

class PropertyEventData
{
    private final String key;
    private final Object value;
    
    public PropertyEventData( String key, Object value )
    {
        this.key = key;
        this.value = value;
    }
    
    public String getKey()
    {
        return key;
    }
    
    public Object getValue()
    {
        return value;
    }
}
