package org.neo4j.logging.internal;

import org.neo4j.logging.AbstractLogProvider;
import org.neo4j.logging.LogProvider;

public class PrefixedLogProvider extends AbstractLogProvider<PrefixedLog>
{
    private final LogProvider logProvider;
    private final String prefix;

    public PrefixedLogProvider( LogProvider logProvider, String prefix )
    {
        this.logProvider = logProvider;
        this.prefix = prefix;
    }

    @Override
    protected PrefixedLog buildLog( Class<?> loggingClass )
    {
        return new PrefixedLog( prefix, logProvider.getLog( loggingClass ) );
    }

    @Override
    protected PrefixedLog buildLog( String name )
    {
        return new PrefixedLog( prefix, logProvider.getLog( name ) );
    }
}
