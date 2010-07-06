package org.neo4j.kernel;

import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;

public interface IdGeneratorFactory
{
    public static final IdGeneratorFactory DEFAULT = new IdGeneratorFactory()
    {
        public IdGenerator open( String fileName, int grabSize, IdType idType )
        {
            return new IdGeneratorImpl( fileName, grabSize );
        }
        
        public void create( String fileName )
        {
            IdGeneratorImpl.createGenerator( fileName );
        }
    };
    
    IdGenerator open( String fileName, int grabSize, IdType idType );
    
    void create( String fileName );
}
