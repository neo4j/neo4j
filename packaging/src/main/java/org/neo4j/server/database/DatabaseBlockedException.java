package org.neo4j.server.database;

/**
 * Thrown when database instantiation has been purposfully blocked and someone
 * tries to instantiate it.
 * 
 * This will occur during server shutdown, or when other actions are performed
 * that need a guarantee that the database is turned off.
 */
public class DatabaseBlockedException extends RuntimeException
{

    /**
     * Serial #
     */
    private static final long serialVersionUID = 3214317342541677412L;

    public DatabaseBlockedException( String message )
    {
        super( message );
    }

}
