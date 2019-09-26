/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext.analyzer;

import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import static org.apache.lucene.analysis.en.EnglishAnalyzer.ENGLISH_STOP_WORDS_SET;

/**
 * Analyzer that uses ASCIIFoldingFilter to remove accents (diacritics).
 * Otherwise behaves as standard english analyzer.
 *
 * Implementation inspired by org.apache.lucene.analysis.standard.StandardAnalyzer
 */
public final class StandardFoldingAnalyzer extends StopwordAnalyzerBase
{
    /** Default maximum allowed token length */
    public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

    public StandardFoldingAnalyzer()
    {
        super( ENGLISH_STOP_WORDS_SET );
    }

    @Override
    protected TokenStreamComponents createComponents( String fieldName )
    {
        StandardTokenizer src = new StandardTokenizer();
        src.setMaxTokenLength( DEFAULT_MAX_TOKEN_LENGTH );
        TokenStream tok = new LowerCaseFilter( src );
        tok = new StopFilter( tok, stopwords );
        tok = new ASCIIFoldingFilter( tok );
        return new TokenStreamComponents( src, tok );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
