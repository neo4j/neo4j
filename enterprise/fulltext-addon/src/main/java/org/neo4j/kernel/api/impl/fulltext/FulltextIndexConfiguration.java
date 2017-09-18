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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FIELD_CONFIG_ANALYZER;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FIELD_CONFIG_PROPERTIES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FIELD_METADATA_DOC;

public class FulltextIndexConfiguration
{
    public static Term TERM = new Term( FIELD_METADATA_DOC );

    private final Set<String> properties;
    private final String analyzerClassName;

    public FulltextIndexConfiguration( Document doc )
    {
        properties = new HashSet<String>( Arrays.asList( doc.getValues( FIELD_CONFIG_PROPERTIES ) ) );
        analyzerClassName = doc.get( FIELD_CONFIG_ANALYZER );
    }

    public FulltextIndexConfiguration( Analyzer analyzer, Set<String> properties )
    {
        this.properties = properties;
        this.analyzerClassName = analyzer.getClass().getCanonicalName();
    }

    public boolean matches( String analyzerClassName, Set<String> properties )
    {
        return this.analyzerClassName.equals( analyzerClassName ) && this.properties.equals( properties );
    }

    public Document asDocument()
    {
        Document doc = new Document();
        doc.add( new StringField( FIELD_METADATA_DOC, "", NO ) );
        doc.add( new StoredField( FIELD_CONFIG_ANALYZER, analyzerClassName ) );
        for ( String property : properties )
        {
            doc.add( new StoredField( FIELD_CONFIG_PROPERTIES, property ) );
        }
        return doc;
    }
}
