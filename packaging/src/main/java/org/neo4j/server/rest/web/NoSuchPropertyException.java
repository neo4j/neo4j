package org.neo4j.server.rest.web;

import org.neo4j.graphdb.PropertyContainer;

//TODO: move this to another package. domain?
public class NoSuchPropertyException extends Exception
{

    public NoSuchPropertyException( PropertyContainer entity, String key )
    {
        super( entity + " does not have a property \"" + key + "\"" );
    }

}
