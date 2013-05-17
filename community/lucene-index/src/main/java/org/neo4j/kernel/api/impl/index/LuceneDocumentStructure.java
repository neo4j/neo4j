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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.neo4j.index.impl.lucene.LuceneUtil;

import static org.apache.lucene.document.Field.Index.NOT_ANALYZED;

import static org.neo4j.index.impl.lucene.IndexType.instantiateField;
import static org.neo4j.index.impl.lucene.IndexType.newBaseDocument;

class LuceneDocumentStructure
{
    private static final String NODE_ID_KEY = "_id_";
    private static final String SINGLE_PROPERTY_KEY = "key";

    Document newDocument( long nodeId, Object value )
    {
        Document document = newBaseDocument( nodeId );
        document.add( new Field( NODE_ID_KEY, "" + nodeId, Field.Store.YES, NOT_ANALYZED ) );
        document.add( instantiateField( SINGLE_PROPERTY_KEY, value, NOT_ANALYZED ) );
        return document;
    }

    public Query newQuery( Object value )
    {
        if ( value instanceof String )
        {
            return new TermQuery( new Term( SINGLE_PROPERTY_KEY, (String) value ) );
        }
        else if ( value instanceof Number )
        {
            Number number = (Number) value;
            return LuceneUtil.rangeQuery( SINGLE_PROPERTY_KEY, number, number, true, true );
        }
        throw new UnsupportedOperationException( value.toString() + ", " + value.getClass() );
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
