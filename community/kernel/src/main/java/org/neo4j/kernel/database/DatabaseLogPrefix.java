package org.neo4j.kernel.database;

public class DatabaseLogPrefix
{
    private DatabaseLogPrefix()
    {
    }

    public static String prefix( NamedDatabaseId namedDatabaseId )
    {
        return namedDatabaseId.name() + "/" + namedDatabaseId.databaseId().id();
    }
}
