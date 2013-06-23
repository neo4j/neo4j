/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.neo4j.index.impl.lucene.LuceneUtil;

import static org.apache.lucene.document.Field.Index.NOT_ANALYZED;
import static org.apache.lucene.document.Field.Store.YES;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.neo4j.index.impl.lucene.IndexType.instantiateField;
import static org.neo4j.index.impl.lucene.IndexType.newBaseDocument;
import static org.neo4j.kernel.api.index.ArrayEncoder.encode;

class LuceneDocumentStructure
{
    private static final String NODE_ID_KEY = "_id_";
    private static final String PROPERTY_FIELD_IDENTIFIER = "key";
    private static final String ARRAY_PROPERTY_FIELD_IDENTIFIER = "array-key";

    Document newDocument( long nodeId, Object value )
    {
        Document document = newBaseDocument( nodeId );
        document.add( new Field( NODE_ID_KEY, "" + nodeId, YES, NOT_ANALYZED ) );

        if ( value.getClass().isArray() )
        {
            document.add( new Field( ARRAY_PROPERTY_FIELD_IDENTIFIER, encode( value ), YES,
                    Field.Index.NOT_ANALYZED ) );
        }
        else if(value instanceof Number)
        {
            document.add( instantiateField( PROPERTY_FIELD_IDENTIFIER, ((Number) value).doubleValue(), NOT_ANALYZED ) );
        }
        else
        {
            document.add( instantiateField( PROPERTY_FIELD_IDENTIFIER, value, NOT_ANALYZED ) );
        }

        return document;
    }

    public Query newQuery( Object value )
    {
        if ( value instanceof Number )
        {
            Number number = (Number) value;
            return LuceneUtil.rangeQuery( PROPERTY_FIELD_IDENTIFIER, number.doubleValue(), number.doubleValue(), true, true );
        }
        else if ( value.getClass().isArray() )
        {
            BooleanQuery booleanClauses = new BooleanQuery();
            booleanClauses.add( new TermQuery( new Term( ARRAY_PROPERTY_FIELD_IDENTIFIER, encode( value ) ) ), MUST );
            return booleanClauses;
        }
        else
        {
            BooleanQuery booleanClauses = new BooleanQuery();
            booleanClauses.add( new TermQuery( new Term( PROPERTY_FIELD_IDENTIFIER, value.toString() ) ), MUST );
            return booleanClauses;
        }
    }

    public Term newQueryForChangeOrRemove( long nodeId )
    {
        return new Term( NODE_ID_KEY, "" + nodeId );
    }

    public long getNodeId( Document from )
    {
        return Long.parseLong( from.get( NODE_ID_KEY ) );
    }
}
