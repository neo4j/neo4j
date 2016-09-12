package org.neo4j.function;

import java.util.Optional;

/**
 * Wrapper around checked exceptions for rethrowing them as runtime exceotions when the signature of the containing method
 * cannot be changed to declare them.
 *
 * Thrown by {@link ThrowingFunction#catchThrown(Class, ThrowingFunction)}
 */
public class UncaughtCheckedException extends RuntimeException
{
    private final Object source;

    public UncaughtCheckedException( Object source, Throwable cause )
    {
        super( "Uncaught checked exception", cause );
        if ( cause == null )
        {
            throw new IllegalArgumentException( "Expected non-null cause" );
        }
        this.source = source;
    }

    /**
     * Check the that the cause has the given type and if succesful, return it.
     *
     * @param clazz class object for the desired type of the cause
     * @param <E> the desired type of the cause
     * @return the underlying cause of this exception but only if it is of desired type E, nothing otherwise
     */
    public <E extends Exception> Optional<E> getCauseIfOfType( Class<E> clazz )
    {
        Throwable cause = getCause();
        if ( clazz.isInstance( cause ) )
        {
            return Optional.of( clazz.cast( cause ) );
        }
        else
        {
            return Optional.empty();
        }
    }

    public Object source()
    {
        return source;
    }
}
