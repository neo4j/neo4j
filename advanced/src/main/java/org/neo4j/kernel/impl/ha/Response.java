package org.neo4j.kernel.impl.ha;

public final class Response<T>
{
    private final T response;
    private final TransactionStream transactions;

    public Response( T response, TransactionStream transactions )
    {
        this.response = response;
        this.transactions = transactions;
    }

    T response() throws MasterFailureException
    {
        return response;
    }

    TransactionStream transactions()
    {
        return transactions;
    }
}
