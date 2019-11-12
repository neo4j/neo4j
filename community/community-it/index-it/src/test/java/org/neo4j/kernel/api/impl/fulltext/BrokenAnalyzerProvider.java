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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.graphdb.schema.AnalyzerProvider;

@ServiceProvider
public class BrokenAnalyzerProvider extends AnalyzerProvider
{
    static final String NAME = "broken-analyzer";
    static volatile boolean shouldThrow;
    static volatile boolean shouldReturnNull;

    public BrokenAnalyzerProvider()
    {
        super( NAME );
    }

    @Override
    public Analyzer createAnalyzer()
    {
        if ( shouldThrow )
        {
            throw new RuntimeException( "boom" );
        }
        if ( shouldReturnNull )
        {
            return null;
        }
        return new StandardAnalyzer();
    }

    @Override
    public String description()
    {
        return "An implementation of the standard analyzer, which can be broken in various ways.";
    }
}
