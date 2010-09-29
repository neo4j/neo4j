package org.neo4j.index.impl.lucene;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.Index;
import org.neo4j.index.impl.IndexStore;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

/**
 * The {@link BatchInserter} version of {@link LuceneIndexProvider}. Indexes
 * created and populated using {@link BatchInserterIndex}s from this provider
 * are compatible with {@link Index}s from {@link LuceneIndexProvider}.
 */
public class LuceneBatchInserterIndexProvider implements BatchInserterIndexProvider
{
    private final BatchInserter inserter;
    private final Map<IndexIdentifier, LuceneBatchInserterIndex> indexes =
            new HashMap<IndexIdentifier, LuceneBatchInserterIndex>();
    final IndexStore indexStore;
    final IndexTypeCache typeCache;
    final EntityType nodeEntityType;
    final EntityType relationshipEntityType;

    public LuceneBatchInserterIndexProvider( final BatchInserter inserter )
    {
        this.inserter = inserter;
        this.indexStore = new IndexStore( ((BatchInserterImpl) inserter).getStore() );
        this.typeCache = new IndexTypeCache();
        this.nodeEntityType = new EntityType()
        {
            public Document newDocument( Object entityId )
            {
                return IndexType.newBaseDocument( (Long) entityId );
            }
            
            public Class<?> getType()
            {
                return Node.class;
            }
        };
        this.relationshipEntityType = new EntityType()
        {
            public Document newDocument( Object entityId )
            {
                RelationshipId relId = (RelationshipId) entityId;
                Document doc = IndexType.newBaseDocument( relId.id );
                doc.add( new Field( LuceneIndex.KEY_START_NODE_ID, "" + relId.startNode,
                        Store.YES, org.apache.lucene.document.Field.Index.NOT_ANALYZED ) );
                doc.add( new Field( LuceneIndex.KEY_END_NODE_ID, "" + relId.endNode,
                        Store.YES, org.apache.lucene.document.Field.Index.NOT_ANALYZED ) );
                return doc;
            }
            
            public Class<?> getType()
            {
                return Relationship.class;
            }
        };
    }
    
    public BatchInserterIndex nodeIndex( String indexName, Map<String, String> config )
    {
        return index( new IndexIdentifier( LuceneCommand.NODE, nodeEntityType, indexName,
                config( indexName, config ) ) );
    }

    private Map<String, String> config( String indexName, Map<String, String> config )
    {
        return indexStore.getIndexConfig( indexName, config, null,
                LuceneConfigDefaultsFiller.INSTANCE ).first();
    }

    public BatchInserterIndex relationshipIndex( String indexName, Map<String, String> config )
    {
        return index( new IndexIdentifier( LuceneCommand.RELATIONSHIP, relationshipEntityType,
                indexName, config( indexName, config ) ) );
    }

    private BatchInserterIndex index( IndexIdentifier identifier )
    {
        // We don't care about threads here... c'mon... it's a
        // single-threaded batch inserter
        LuceneBatchInserterIndex index = indexes.get( identifier );
        if ( index == null )
        {
            index = new LuceneBatchInserterIndex( this, inserter, identifier );
            indexes.put( identifier, index );
        }
        return index;
    }
    
    public void shutdown()
    {
        for ( LuceneBatchInserterIndex index : indexes.values() )
        {
            index.shutdown();
        }
    }
}
