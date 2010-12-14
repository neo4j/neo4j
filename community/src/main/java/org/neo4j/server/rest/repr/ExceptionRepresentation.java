package org.neo4j.server.rest.repr;

class ExceptionRepresentation extends MappingRepresentation
{
    private final Throwable exception;

    ExceptionRepresentation( Throwable exception )
    {
        super( RepresentationType.EXCEPTION );
        this.exception = exception;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putString( "message", exception.getMessage() );
    }
}
