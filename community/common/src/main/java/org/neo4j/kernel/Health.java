package org.neo4j.kernel;

public interface Health
{
    /**
     * Asserts that the database is in good health. If that is not the case then the cause of the
     * unhealthy state is wrapped in an exception of the given type, i.e. the panic disguise.
     *
     * @param panicDisguise the cause of the unhealthy state wrapped in an exception of this type.
     * @throws EXCEPTION exception type to wrap cause in.
     */
    <EXCEPTION extends Throwable> void assertHealthy( Class<EXCEPTION> panicDisguise ) throws EXCEPTION;

    void panic( Throwable cause );

    boolean isHealthy();

    void healed();

    Throwable cause();
}
