package org.neo4j.server.rest.web;

//TODO: move this to another package. domain?
public class PropertyValueException extends Exception
{
    public PropertyValueException( String key, Object value )
    {
        super( "Could not set property \"" + key + "\", unsupported type: " + value );
    }

}
