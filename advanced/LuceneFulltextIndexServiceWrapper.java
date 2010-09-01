package org.neo4j.index.impl.lucene;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

class LuceneFulltextIndexServiceWrapper extends LuceneIndexServiceWrapper
{
    static final Analyzer LOWER_CASE_WHITESPACE_ANALYZER = new Analyzer()
    {
        @Override
        public TokenStream tokenStream( String fieldName, Reader reader )
        {
            return new LowerCaseFilter( new WhitespaceTokenizer( reader ) );
        }
    };
    
    public LuceneFulltextIndexServiceWrapper( LuceneIndexProvider provider )
    {
        super( provider );
    }
    
    @Override
    protected Index<Node> getNodeIndex( String key )
    {
        // TODO Make sure the Index which key refers to is a fulltext index,
        // maybe also with a prefix or something so that fulltext and
        // non-fulltext (with same key) can live side by side, as they
        // could in the old implementation.
        return provider.nodeIndex( key, LuceneIndexProvider.FULLTEXT_CONFIG );
    }

    @Override
    public IndexHits<Node> getNodes( String key, Object value )
    {
        return getIndex( key ).query( key, toQuery( key, value ) );
    }
    
    public static Object toQuery( String key, Object value )
    {
        TokenStream stream = LOWER_CASE_WHITESPACE_ANALYZER.tokenStream(
                key, new StringReader( value.toString().toLowerCase() ) );
        BooleanQuery booleanQuery = new BooleanQuery();
        try
        {
            while ( stream.incrementToken() )
            {
                String term = stream.getAttribute( TermAttribute.class ).term();
                booleanQuery.add( new TermQuery( new Term( key, term ) ),
                        Occur.MUST );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return booleanQuery;
    }
}
