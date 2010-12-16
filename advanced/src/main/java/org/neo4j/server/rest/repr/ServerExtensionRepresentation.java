package org.neo4j.server.rest.repr;

import java.util.List;

public final class ServerExtensionRepresentation extends MappingRepresentation
{
    private final List<ExtensionPointRepresentation> methods;

    public ServerExtensionRepresentation( String name, List<ExtensionPointRepresentation> methods )
    {
        super( RepresentationType.SERVER_EXTENSION_DESCRIPTION );
        this.methods = methods;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putList( "extension-points", new ListRepresentation(
                RepresentationType.EXTENSION_POINT_DESCRIPTION, methods ) );
    }
}
