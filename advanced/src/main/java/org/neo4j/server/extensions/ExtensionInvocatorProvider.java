package org.neo4j.server.extensions;

import org.neo4j.server.database.AbstractInjectableProvider;

import com.sun.jersey.api.core.HttpContext;

public class ExtensionInvocatorProvider extends AbstractInjectableProvider<ExtensionInvocator>
{
    private final ExtensionInvocator invocator;

    public ExtensionInvocatorProvider( ExtensionInvocator invocator )
    {
        super( ExtensionInvocator.class );
        this.invocator = invocator;
    }

    @Override
    public ExtensionInvocator getValue( HttpContext c )
    {
        return invocator;
    }
}
