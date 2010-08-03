package org.neo4j.kernel.impl.ha;

public class FailedResponse<T> extends Response<T>
{
    public FailedResponse()
    {
        super( null, null );
    }
    
    @Override
    public T response() throws MasterFailureException
    {
        throw new MasterFailureException();
    }
}
