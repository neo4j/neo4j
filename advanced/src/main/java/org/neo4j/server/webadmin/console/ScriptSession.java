package org.neo4j.server.webadmin.console;

import java.util.List;

public interface ScriptSession
{
    @SuppressWarnings( "unchecked" )
    List<String> evaluate( String script );
}
