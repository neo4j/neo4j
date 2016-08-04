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
package org.neo4j.kernel.api.impl.labelscan;

import org.neo4j.kernel.api.impl.index.builder.AbstractLuceneIndexBuilder;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;

/**
 * Helper builder class to simplify construction and instantiation of lucene label scan indexes.
 * Most of the values already have most useful default value, that still can be overridden by corresponding
 * builder methods.
 */
public class LuceneLabelScanIndexBuilder extends AbstractLuceneIndexBuilder<LuceneLabelScanIndexBuilder>
{
    public static final String DEFAULT_INDEX_IDENTIFIER = "labelStore";

    private BitmapDocumentFormat format = BitmapDocumentFormat._32;

    private LuceneLabelScanIndexBuilder()
    {
        super();
        storageBuilder.withIndexIdentifier( DEFAULT_INDEX_IDENTIFIER );
    }

    /**
     * Create new label scan store builder
     *
     * @return index builder
     */
    public static LuceneLabelScanIndexBuilder create()
    {
        return new LuceneLabelScanIndexBuilder();
    }

    /**
     * Specify label scan store format
     *
     * @param format document format
     * @return index builder
     */
    public LuceneLabelScanIndexBuilder withDocumentFormat( BitmapDocumentFormat format )
    {
        this.format = format;
        return this;
    }

    /**
     * Build lucene label scan index with specified configuration
     *
     * @return lucene label scan index
     */
    public LabelScanIndex build()
    {
        return isReadOnly() ? new ReadOnlyDatabaseLabelScanIndex( format, storageBuilder.build() )
                            : new WritableDatabaseLabelScanIndex( format, storageBuilder.build() );
    }
}
