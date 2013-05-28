package org.neo4j.kernel.api.exceptions.index;

import org.neo4j.kernel.api.exceptions.KernelException;

public final class FlipFailedKernelException extends KernelException
{
    public FlipFailedKernelException( Throwable cause )
    {
        super( cause, "Failed to transition index to new context: %s", cause.getMessage() );
    }
}
