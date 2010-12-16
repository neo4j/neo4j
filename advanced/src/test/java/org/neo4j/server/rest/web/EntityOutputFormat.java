package org.neo4j.server.rest.web;

import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.Representation;

import javax.ws.rs.core.Response;

public class EntityOutputFormat extends OutputFormat
{
    private Representation representation;

    public EntityOutputFormat(  )
    {
        super( null, null, null );
    }

    @Override
    protected Response response( Response.ResponseBuilder response, Representation representation )
    {
        this.representation = representation;

        return response.build();
    }

    public Representation getRepresentation()
    {
        return representation;
    }
}
