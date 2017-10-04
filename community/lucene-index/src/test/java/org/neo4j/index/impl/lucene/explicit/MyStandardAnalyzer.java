/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.index.impl.lucene.explicit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;

import java.util.Arrays;
import java.util.HashSet;

public final class MyStandardAnalyzer extends AnalyzerWrapper
{
    private final Analyzer actual;

    public MyStandardAnalyzer()
    {
        super( GLOBAL_REUSE_STRATEGY );
        CharArraySet stopWords = CharArraySet.copy( new HashSet<String>( Arrays.asList( "just", "some", "words" ) ) );
        actual = new StandardAnalyzer( stopWords );
    }

    @Override
    protected Analyzer getWrappedAnalyzer( String fieldName )
    {
        return actual;
    }
}
