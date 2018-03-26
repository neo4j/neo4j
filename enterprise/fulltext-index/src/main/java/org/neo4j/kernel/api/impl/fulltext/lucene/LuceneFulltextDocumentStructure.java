/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.values.storable.Value;

import static org.apache.lucene.document.Field.Store.NO;

public class LuceneFulltextDocumentStructure
{
    private static final ThreadLocal<DocWithId> perThreadDocument = ThreadLocal.withInitial( DocWithId::new );

    private LuceneFulltextDocumentStructure()
    {
    }

    private static DocWithId reuseDocument( long id )
    {
        DocWithId doc = perThreadDocument.get();
        doc.setId( id );
        return doc;
    }

    public static Document documentRepresentingProperties( long id, Collection<String> propertyNames, Value[] values )
    {
        DocWithId document = reuseDocument( id );
        document.setValues( propertyNames, values );
        return document.document;
    }

    private static Field encodeValueField( String propertyKey, Value value )
    {
        String stringValue = value.prettyPrint();

        TextField field = new TextField( propertyKey, stringValue, NO );
        return field;
    }

    public static Term newTermForChangeOrRemove( long id )
    {
        return new Term( FulltextAdapter.FIELD_ENTITY_ID, "" + id );
    }

    private static class DocWithId
    {
        private final Document document;

        private final Field idField;
        private final Field idValueField;

        private DocWithId()
        {
            idField = new StringField( FulltextAdapter.FIELD_ENTITY_ID, "", NO );
            idValueField = new NumericDocValuesField( FulltextAdapter.FIELD_ENTITY_ID, 0L );
            document = new Document();
            document.add( idField );
            document.add( idValueField );
        }

        private void setId( long id )
        {
            removeAllValueFields();
            idField.setStringValue( Long.toString( id ) );
            idValueField.setLongValue( id );
        }

        private void setValues( Map<String,Value> values )
        {
            for ( Map.Entry<String,Value> entry : values.entrySet() )
            {
                Field field = encodeValueField( entry.getKey(), entry.getValue() );
                document.add( field );
            }
        }

        private void setValues( Collection<String> names, Value[] values )
        {
            int i = 0;
            for ( String name : names )
            {
                Value value = values[i++];
                if ( value != null )
                {
                    Field field = encodeValueField( name, value );
                    document.add( field );
                }
            }
        }

        private void removeAllValueFields()
        {
            Iterator<IndexableField> it = document.getFields().iterator();
            while ( it.hasNext() )
            {
                IndexableField field = it.next();
                String fieldName = field.name();
                if ( !fieldName.equals( FulltextAdapter.FIELD_ENTITY_ID ) )
                {
                    it.remove();
                }
            }
        }
    }
}
