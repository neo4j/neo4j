/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.io.IOException;

import org.apache.lucene.document.Document;

public class HitsIterator extends AbstractIndexHits<Document>
{
    private final int size;
    private final Hits hits;
    private int index;

    public HitsIterator( Hits hits )
    {
        this.size = hits.length();
        this.hits = hits;
    }

    @Override
    protected Document fetchNextOrNull()
    {
        int i = index++;
        try
        {
            return i < size() ? hits.doc( i ) : null;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public float currentScore()
    {
        int i = index-1;
        try
        {
            return i >= 0 && i < size() ? hits.score( i ) : -1;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public int size()
    {
        return this.size;
    }
}
