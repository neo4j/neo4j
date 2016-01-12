/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index.builder;

import org.neo4j.kernel.api.impl.index.BitmapDocumentFormat;
import org.neo4j.kernel.api.impl.index.LuceneLabelScanIndex;

public class LuceneLabelScanIndexBuilder extends AbstractLuceneIndexBuilder<LuceneLabelScanIndexBuilder>
{
    public static final String DEFAULT_INDEX_IDENTIFIER = "labelStore";

    private BitmapDocumentFormat format = BitmapDocumentFormat._32;

    private LuceneLabelScanIndexBuilder()
    {
        super();
        storageBuilder.withIndexIdentifier( DEFAULT_INDEX_IDENTIFIER );
    }

    public static LuceneLabelScanIndexBuilder create()
    {
        return new LuceneLabelScanIndexBuilder();
    }

    public LuceneLabelScanIndexBuilder withDocumentFormat( BitmapDocumentFormat format )
    {
        this.format = format;
        return this;
    }

    public LuceneLabelScanIndex build()
    {
        return new LuceneLabelScanIndex( format, storageBuilder.buildIndexStorage() );
    }
}
