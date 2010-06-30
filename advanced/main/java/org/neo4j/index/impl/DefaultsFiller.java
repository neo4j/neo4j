package org.neo4j.index.impl;


import java.util.Map;

public interface DefaultsFiller
{
    Map<String, String> fill( Map<String, String> source );
}
