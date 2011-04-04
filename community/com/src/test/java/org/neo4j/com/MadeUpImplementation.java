package org.neo4j.com;

import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class MadeUpImplementation implements MadeUpCommunicationInterface
{
    private final StoreId storeIdToRespondWith;
    private boolean gotCalled;

    public MadeUpImplementation( StoreId storeIdToRespondWith )
    {
        this.storeIdToRespondWith = storeIdToRespondWith;
    }
    
    @Override
    public Response<Integer> multiply( int value1, int value2 )
    {
        gotCalled = true;
        return new Response<Integer>( value1*value2, storeIdToRespondWith, TransactionStream.EMPTY );
    }
    
    @Override
    public Response<Void> streamSomeData( MadeUpWriter writer, int dataSize )
    {
        writer.write( new KnownDataByteChannel( dataSize ) );
        return new Response<Void>( null, storeIdToRespondWith, TransactionStream.EMPTY );
    }
    
    public boolean gotCalled()
    {
        return this.gotCalled;
    }
}
