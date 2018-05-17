/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.apache.lucene.document.Field.Store.NO;

class LuceneFulltextDocumentStructure
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

    static Document documentRepresentingProperties( long id, Map<String,Object> values )
    {
        DocWithId document = reuseDocument( id );
        document.setValues( values );
        return document.document;
    }

    static Field encodeValueField( String propertyKey, Value value )
    {
        return LuceneFulltextFieldEncoding.encodeField( propertyKey, value );
    }

    private static class DocWithId
    {
        private final Document document;

        private final Field idField;
        private final Field idValueField;

        private DocWithId()
        {
            idField = new StringField( FulltextProvider.FIELD_ENTITY_ID, "", NO );
            idValueField = new NumericDocValuesField( FulltextProvider.FIELD_ENTITY_ID, 0L );
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

        private void setValues( Map<String,Object> values )
        {
            for ( Map.Entry<String,Object> entry : values.entrySet() )
            {
                Field field = encodeValueField( entry.getKey(), Values.of( entry.getValue() ) );
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
                if ( !fieldName.equals( FulltextProvider.FIELD_ENTITY_ID ) )
                {
                    it.remove();
                }
            }
        }

    }

    static Term newTermForChangeOrRemove( long id )
    {
        return new Term( FulltextProvider.FIELD_ENTITY_ID, "" + id );
    }
}
