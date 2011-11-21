package org.neo4j.index.impl.lucene;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;

public class LowerCaseKeywordAnalyzer extends Analyzer
{
    @Override
    public TokenStream tokenStream( String fieldName, Reader reader )
    {
        return new LowerCaseFilter( LuceneDataSource.LUCENE_VERSION, new KeywordTokenizer( reader ) );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
};
