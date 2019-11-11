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

import java.util.Objects;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.schema.AnalyzerProvider;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.service.Services;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.ANALYZER;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT;

final class FulltextIndexSettings
{
    private FulltextIndexSettings()
    {}

    static Analyzer createAnalyzer( IndexDescriptor descriptor, TokenNameLookup tokenNameLookup )
    {
        TextValue analyzerName = descriptor.getIndexConfig().get( ANALYZER );
        if ( analyzerName == null )
        {
            throw new RuntimeException( "Index has no analyzer configured: " + descriptor.userDescription( tokenNameLookup ) );
        }
        Analyzer analyzer;
        try
        {
            AnalyzerProvider analyzerProvider = Services.loadOrFail( AnalyzerProvider.class, analyzerName.stringValue() );
            analyzer = analyzerProvider.createAnalyzer();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create fulltext analyzer: " + analyzerName, e );
        }
        Objects.requireNonNull( analyzer, "The '" + analyzerName + "' analyzer provider returned a null analyzer." );
        return analyzer;
    }

    static String[] createPropertyNames( IndexDescriptor descriptor, TokenNameLookup tokenNameLookup )
    {
        int[] propertyIds = descriptor.schema().getPropertyIds();
        String[] propertyNames = new String[propertyIds.length];
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            propertyNames[i] = tokenNameLookup.propertyKeyGetName( propertyIds[i] );
        }
        return propertyNames;
    }

    static boolean isEventuallyConsistent( IndexDescriptor index )
    {
        BooleanValue eventuallyConsistent = index.getIndexConfig().getOrDefault( EVENTUALLY_CONSISTENT, BooleanValue.FALSE );
        return eventuallyConsistent.booleanValue();
    }
}
