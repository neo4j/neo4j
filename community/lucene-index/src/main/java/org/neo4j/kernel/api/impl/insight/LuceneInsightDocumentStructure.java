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
package org.neo4j.kernel.api.impl.insight;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;

import java.util.Iterator;

import org.neo4j.kernel.api.properties.PropertyKeyValue;
import org.neo4j.values.storable.Value;

import static org.apache.lucene.document.Field.Store.YES;

class LuceneInsightDocumentStructure
{
    static final String NODE_ID_KEY = "id";

    private static final ThreadLocal<DocWithId> perThreadDocument = ThreadLocal.withInitial( DocWithId::new );

    private LuceneInsightDocumentStructure()
    {
    }

    private static DocWithId reuseDocument( long nodeId )
    {
        DocWithId doc = perThreadDocument.get();
        doc.setId( nodeId );
        return doc;
    }

    public static Document documentRepresentingProperties( long nodeId, PropertyKeyValue... values )
    {
        DocWithId document = reuseDocument( nodeId );
        document.setValues( values );
        return document.document;
    }

    public static long getNodeId( Document from )
    {
        return Long.parseLong( from.get( NODE_ID_KEY ) );
    }

    static Field encodeValueField( int propertyNumber, Value value )
    {
        InsightFieldEncoding encoding = InsightFieldEncoding.forValue( value );
        return encoding.encodeField( Integer.toString( propertyNumber ), value );
    }

    private static class DocWithId
    {
        private final Document document;

        private final Field idField;
        private final Field idValueField;

        private DocWithId()
        {
            idField = new StringField( NODE_ID_KEY, "", YES );
            idValueField = new NumericDocValuesField( NODE_ID_KEY, 0L );
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

        private void setValues( PropertyKeyValue... values )
        {
            for ( int i = 0; i < values.length; i++ )
            {
                Field field = encodeValueField( values[i].propertyKeyId(), values[i].value() );
                document.add( field );
            }
        }

        private void removeAllValueFields()
        {
            Iterator<IndexableField> it = document.getFields().iterator();
            while ( it.hasNext() )
            {
                IndexableField field = it.next();
                String fieldName = field.name();
                if ( !fieldName.equals( NODE_ID_KEY ) )
                {
                    it.remove();
                }
            }
        }

    }
}
