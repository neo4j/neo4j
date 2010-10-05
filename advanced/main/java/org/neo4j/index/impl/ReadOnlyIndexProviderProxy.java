package org.neo4j.index.impl;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.impl.lucene.LuceneIndexProvider;
import org.neo4j.shell.kernel.ReadOnlyGraphDatabaseProxy;

class ReadOnlyIndexProviderProxy extends IndexProvider
{
    private final ReadOnlyGraphDatabaseProxy graphDb;
    private final LuceneIndexProvider indexes;

    ReadOnlyIndexProviderProxy( ReadOnlyGraphDatabaseProxy proxy, LuceneIndexProvider indexes )
    {
        super( null );
        this.graphDb = proxy;
        this.indexes = indexes;
    }

    @Override
    public Index<Node> nodeIndex( String indexName, Map<String, String> config )
    {
        return new ReadOnlyIndexProxy<Node, Index<Node>>( indexes.nodeIndex( indexName, config ) )
        {
            @Override
            Node wrap( Node actual )
            {
                return graphDb.readOnly( actual );
            }
        };
    }

    @Override
    public RelationshipIndex relationshipIndex( String indexName, Map<String, String> config )
    {
        return new ReadOnlyRelationshipIndexProxy( indexes.relationshipIndex( indexName, config ) );
    }

    private static <T> T readOnly()
    {
        throw new UnsupportedOperationException( "Read only Graph Database Index!" );
    }

    private abstract class ReadOnlyIndexProxy<T extends PropertyContainer, I extends Index<T>>
            implements Index<T>
    {
        final I actual;

        ReadOnlyIndexProxy( I actual )
        {
            this.actual = actual;
        }

        abstract T wrap( T actual );

        public void clear()
        {
            readOnly();
        }

        public void add( T entity, String key, Object value )
        {
            readOnly();
        }

        public IndexHits<T> get( String key, Object value )
        {
            return new ReadOnlyIndexHitsProxy<T>( this, actual.get( key, value ) );
        }

        public IndexHits<T> query( String key, Object queryOrQueryObject )
        {
            return new ReadOnlyIndexHitsProxy<T>( this, actual.query( key, queryOrQueryObject ) );
        }

        public IndexHits<T> query( Object queryOrQueryObject )
        {
            return new ReadOnlyIndexHitsProxy<T>( this, actual.query( queryOrQueryObject ) );
        }

        public void remove( T entity, String key, Object value )
        {
            readOnly();
        }
    }

    private class ReadOnlyRelationshipIndexProxy extends
            ReadOnlyIndexProxy<Relationship, RelationshipIndex>
            implements RelationshipIndex
    {
        ReadOnlyRelationshipIndexProxy( RelationshipIndex actual )
        {
            super( actual );
        }

        @Override
        Relationship wrap( Relationship actual )
        {
            return graphDb.readOnly( actual );
        }

        public IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull,
                Node endNodeOrNull )
        {
            return new ReadOnlyIndexHitsProxy<Relationship>( this, actual.get( key, valueOrNull,
                    startNodeOrNull, endNodeOrNull ) );
        }

        public IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull,
                Node startNodeOrNull, Node endNodeOrNull )
        {
            return new ReadOnlyIndexHitsProxy<Relationship>( this, actual.query( key,
                    queryOrQueryObjectOrNull, startNodeOrNull, endNodeOrNull ) );
        }

        public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull,
                Node startNodeOrNull, Node endNodeOrNull )
        {
            return new ReadOnlyIndexHitsProxy<Relationship>( this, actual.query(
                    queryOrQueryObjectOrNull, startNodeOrNull, endNodeOrNull ) );
        }
    }

    private static class ReadOnlyIndexHitsProxy<T extends PropertyContainer> implements
            IndexHits<T>
    {
        private final ReadOnlyIndexProxy<T, ?> index;
        private final IndexHits<T> actual;

        ReadOnlyIndexHitsProxy( ReadOnlyIndexProxy<T, ?> index, IndexHits<T> actual )
        {
            this.index = index;
            this.actual = actual;
        }

        public void close()
        {
            actual.close();
        }

        public T getSingle()
        {
            return index.wrap( actual.getSingle() );
        }

        public int size()
        {
            return actual.size();
        }

        public boolean hasNext()
        {
            return actual.hasNext();
        }

        public T next()
        {
            return index.wrap( actual.next() );
        }

        public void remove()
        {
            readOnly();
        }

        public Iterator<T> iterator()
        {
            return this;
        }
    }
}
