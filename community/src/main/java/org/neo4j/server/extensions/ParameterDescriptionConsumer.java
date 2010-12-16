package org.neo4j.server.extensions;

public interface ParameterDescriptionConsumer
{
    void describeParameter( String name, Class<?> type, boolean optional, String description );

    void describeListParameter( String name, Class<?> type, boolean optional, String description );
}
