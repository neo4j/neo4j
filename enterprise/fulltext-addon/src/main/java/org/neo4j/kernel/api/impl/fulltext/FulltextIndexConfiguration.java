/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongField;
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
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FIELD_LAST_COMMITTED_TX_ID;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FIELD_METADATA_DOC;

class FulltextIndexConfiguration
{
    static Term TERM = new Term( FIELD_METADATA_DOC );

    private final Set<String> properties;
    private final String analyzerClassName;
    private final long txId;

    FulltextIndexConfiguration( Document doc )
    {
        properties = new HashSet<>( Arrays.asList( doc.getValues( FIELD_CONFIG_PROPERTIES ) ) );
        analyzerClassName = doc.get( FIELD_CONFIG_ANALYZER );
        txId = Long.parseLong( doc.get( FIELD_LAST_COMMITTED_TX_ID ) );
    }

    FulltextIndexConfiguration( String analyzerClassName, Set<String> properties, long txId )
    {
        this.properties = properties;
        this.analyzerClassName = analyzerClassName;
        this.txId = txId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        FulltextIndexConfiguration that = (FulltextIndexConfiguration) o;

        return txId == that.txId && properties.equals( that.properties ) &&
               analyzerClassName.equals( that.analyzerClassName );
    }

    @Override
    public int hashCode()
    {
        int result = properties.hashCode();
        result = 31 * result + analyzerClassName.hashCode();
        result = 31 * result + (int) (txId ^ (txId >>> 32));
        return result;
    }

    Document asDocument()
    {
        Document doc = new Document();
        doc.add( new StringField( FIELD_METADATA_DOC, "", NO ) );
        doc.add( new StoredField( FIELD_CONFIG_ANALYZER, analyzerClassName ) );
        doc.add( new LongField( FIELD_LAST_COMMITTED_TX_ID, txId, YES ) );
        for ( String property : properties )
        {
            doc.add( new StoredField( FIELD_CONFIG_PROPERTIES, property ) );
        }
        return doc;
    }
}
