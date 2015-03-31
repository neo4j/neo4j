package org.neo4j.server.rest.repr;

import org.neo4j.kernel.api.exceptions.Status;

public class InvalidArgumentsException extends BadInputException
{
    public InvalidArgumentsException( String message )
    {
        super( message );
    }

    @Override
    public Status status()
    {
        return Status.Statement.InvalidArguments;
    }
}
