package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository;

public class NodeImporter implements Runnable
{
    private final InputIterator data;
    private final NodeVisitor nodeVisitor;

    public NodeImporter( InputIterator data, NeoStores stores,
            BatchingTokenRepository.BatchingPropertyKeyTokenRepository propertyKeyTokenRepository,
            BatchingTokenRepository.BatchingLabelTokenRepository labelTokenRepository,
            IdMapper idMapper )
    {
        this.data = data;
        this.nodeVisitor = new NodeVisitor( stores, propertyKeyTokenRepository, labelTokenRepository, idMapper );
    }

    @Override
    public void run()
    {
        try
        {
            InputChunk chunk = data.newChunk();
            while ( data.next( chunk ) )
            {
                while ( chunk.next( nodeVisitor ) );
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }
}
