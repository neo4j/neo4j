package org.neo4j.index.impl.lucene;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

class CustomAnalyzer extends Analyzer
{
    static boolean called;
    
    @Override
    public TokenStream tokenStream( String fieldName, Reader reader )
    {
        called = true;
        return new LowerCaseFilter( new WhitespaceTokenizer( reader ) );
    }
}
