/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;

import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;

public class LuceneFulltextDocumentStructure
{
    public static final String FIELD_ENTITY_ID = "__neo4j__lucene__fulltext__index__internal__id__";

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

    /**
     * @return A document with the properties set, or null if no properties were
     * relevant (= none of the properties were of type TEXT - which is the only type we support in the fulltext indexes).
     */
    public static Document documentRepresentingProperties( long id, String[] propertyNames, Value[] values )
    {
        DocWithId document = reuseDocument( id );
        int setValues = document.setValues( propertyNames, values );
        return setValues == 0 ? null : document.document;
    }

    private static Field encodeValueField( String propertyKey, Value value )
    {
        TextValue textValue = (TextValue) value;
        String stringValue = textValue.stringValue();
        return new TextField( propertyKey, stringValue, NO );
    }

    static long getNodeId( Document from )
    {
        String entityId = from.get( FIELD_ENTITY_ID );
        return Long.parseLong( entityId );
    }

    static Term newTermForChangeOrRemove( long id )
    {
        return new Term( FIELD_ENTITY_ID, "" + id );
    }

    static Query newCountEntityEntriesQuery( long nodeId, String[] propertyKeys, Value... propertyValues )
    {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add( new TermQuery( newTermForChangeOrRemove( nodeId ) ), BooleanClause.Occur.MUST );
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            String propertyKey = propertyKeys[i];
            Value value = propertyValues[i];
            // Only match on entries that doesn't contain fields we don't expect
            if ( value.valueGroup() != ValueGroup.TEXT )
            {
                Query valueQuery = new ConstantScoreQuery(
                        new WildcardQuery( new Term( propertyKey, "*" ) ) );
                builder.add( valueQuery, BooleanClause.Occur.MUST_NOT );
            }
            // Why don't we match on the TEXT values that actually should be in the index?
            // 1. The analyzer used for our index can have split the property value into several terms so we cannot
            //    check that the exact property value exist in the index.
            // 2. There are some characters that analyzers will skip completely and if we have a property value with
            //    only such characters there will be no reference to the field at all, so we cannot use a wildcard query either.
        }
        return builder.build();
    }

    private static class DocWithId
    {
        private final Document document;

        private final Field idField;
        private final Field idValueField;

        private DocWithId()
        {
            idField = new StringField( FIELD_ENTITY_ID, "", YES );
            idValueField = new NumericDocValuesField( FIELD_ENTITY_ID, 0L );
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

        private int setValues( String[] names, Value[] values )
        {
            int i = 0;
            int nbrAddedValues = 0;
            for ( String name : names )
            {
                Value value = values[i++];
                if ( value != null && value.valueGroup() == ValueGroup.TEXT )
                {
                    Field field = encodeValueField( name, value );
                    document.add( field );
                    nbrAddedValues++;
                }
            }
            return nbrAddedValues;
        }

        private void removeAllValueFields()
        {
            document.clear();
            document.add( idField );
            document.add( idValueField );
        }
    }
}
