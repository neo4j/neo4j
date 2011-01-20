package org.neo4j.server.database;

import java.util.Map;

import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;

public enum DatabaseMode
{
    EMBEDDED
    {
        @Override
        AbstractGraphDatabase createDatabase( String databaseStoreDirectory,
                Map<String, String> databaseTuningProperties )
        {
            return new EmbeddedGraphDatabase( databaseStoreDirectory, databaseTuningProperties );
        }
    },
    HA
    {
        @Override
        AbstractGraphDatabase createDatabase( String databaseStoreDirectory,
                Map<String, String> databaseTuningProperties )
        {
            return new HighlyAvailableGraphDatabase( databaseStoreDirectory,
                    databaseTuningProperties );
        }
    };

    abstract AbstractGraphDatabase createDatabase( String databaseStoreDirectory,
            Map<String, String> databaseTuningProperties );
}
