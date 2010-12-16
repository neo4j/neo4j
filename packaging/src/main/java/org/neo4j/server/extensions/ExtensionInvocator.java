package org.neo4j.server.extensions;

import java.util.List;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.ExtensionPointRepresentation;
import org.neo4j.server.rest.repr.Representation;

public interface ExtensionInvocator
{
    <T> Representation invoke( AbstractGraphDatabase graphDb, String name, Class<T> type,
            String method, T context, ParameterList params ) throws ExtensionLookupException,
            BadInputException, ExtensionInvocationFailureException, BadExtensionInvocationException;

    Representation describe( String name, Class<?> type, String method )
            throws ExtensionLookupException;

    List<ExtensionPointRepresentation> describeAll( String extensionName )
            throws ExtensionLookupException;
}
