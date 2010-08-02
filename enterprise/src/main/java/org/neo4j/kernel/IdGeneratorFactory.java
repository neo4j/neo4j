package org.neo4j.kernel;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;

public interface IdGeneratorFactory
{
    public static final IdGeneratorFactory DEFAULT = new IdGeneratorFactory()
    {
        private final Map<IdType, IdGenerator> generators = new HashMap<IdType, IdGenerator>();
        
        public IdGenerator open( String fileName, int grabSize, IdType idType,
                long highestIdInUse )
        {
            IdGenerator generator = new IdGeneratorImpl( fileName, grabSize );
            generators.put( idType, generator );
            return generator;
        }
        
        public IdGenerator get( IdType idType )
        {
            return generators.get( idType );
        }
        
        public void create( String fileName )
        {
            IdGeneratorImpl.createGenerator( fileName );
        }
    };
    
    IdGenerator open( String fileName, int grabSize, IdType idType, long highestIdInUse );
    
    void create( String fileName );
    
    IdGenerator get( IdType idType );
}
