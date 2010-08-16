package org.neo4j.kernel;

import org.neo4j.kernel.impl.nioneo.store.IdGenerator;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public interface IdGeneratorFactory
{
    IdGenerator open( String fileName, int grabSize, IdType idType, long highestIdInUse );
    
    void create( String fileName );
    
    IdGenerator get( IdType idType );
    
    void updateIdGenerators( NeoStore store );
}
