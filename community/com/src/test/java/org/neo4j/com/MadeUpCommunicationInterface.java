package org.neo4j.com;

public interface MadeUpCommunicationInterface
{
    Response<Integer> multiply( int value1, int value2 );
    
    Response<Void> streamSomeData( MadeUpWriter writer, int dataSize );
}
