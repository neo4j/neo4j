package org.neo4j.kernel.impl.ha;

public class Response<T>
{
    private final T response;
    private final TransactionStreams transactions;

    public Response( T response, TransactionStreams transactions )
    {
        this.response = response;
        this.transactions = transactions;
    }

    public T response() throws MasterFailureException
    {
        return response;
    }

    public TransactionStreams transactions()
    {
        return transactions;
    }
}
