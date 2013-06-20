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

import static org.apache.lucene.document.Field.Index.NOT_ANALYZED;
import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.document.Field.Store.YES;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;
import static org.neo4j.index.impl.lucene.IndexType.instantiateField;
import static org.neo4j.index.impl.lucene.IndexType.newBaseDocument;
import static org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ArrayEncoder.encode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.neo4j.index.impl.lucene.LuceneUtil;
import sun.misc.BASE64Encoder;

class LuceneDocumentStructure
{
    private static final String NODE_ID_KEY = "_id_";
    private static final String TYPE_FIELD_IDENTIFIER = "type";
    private static final String PROPERTY_FIELD_IDENTIFIER = "key";
    private static final String ARRAY = "array";
    private static final String NOT_ARRAY = "not-array";

    Document newDocument( long nodeId, Object value )
    {
        Document document = newBaseDocument( nodeId );
        document.add( new Field( NODE_ID_KEY, "" + nodeId, YES, NOT_ANALYZED ) );

        if ( value.getClass().isArray() )
        {
            document.add( new Field( TYPE_FIELD_IDENTIFIER, ARRAY, NO, NOT_ANALYZED ) );
            document.add( new Field( PROPERTY_FIELD_IDENTIFIER, encode( value ), YES,
                    Field.Index.NOT_ANALYZED ) );
        }
        else
        {
            document.add( new Field( TYPE_FIELD_IDENTIFIER, NOT_ARRAY, NO, NOT_ANALYZED ) );
            document.add( instantiateField( PROPERTY_FIELD_IDENTIFIER, value, NOT_ANALYZED ) );
        }

        return document;
    }

    public Query newQuery( Object value )
    {
        if ( value instanceof Number )
        {
            Number number = (Number) value;
            return LuceneUtil.rangeQuery( PROPERTY_FIELD_IDENTIFIER, number, number, true, true );
        }
        else if ( value.getClass().isArray() )
        {
            BooleanQuery booleanClauses = new BooleanQuery();
            booleanClauses.add( new TermQuery( new Term( TYPE_FIELD_IDENTIFIER, ARRAY ) ), MUST );
            booleanClauses.add( new TermQuery( new Term( PROPERTY_FIELD_IDENTIFIER, encode( value ) ) ), MUST );
            return booleanClauses;
        }
        else
        {
            BooleanQuery booleanClauses = new BooleanQuery();
            booleanClauses.add( new TermQuery( new Term( TYPE_FIELD_IDENTIFIER, NOT_ARRAY ) ), MUST );
            booleanClauses.add( new TermQuery( new Term( PROPERTY_FIELD_IDENTIFIER, (String) value ) ), MUST );
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

    static class ArrayEncoder
    {
        private static final BASE64Encoder base64Encoder = new BASE64Encoder();

        static String encode( Object value )
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            try
            {
                out = new ObjectOutputStream( bos );
                out.writeObject( value );
                out.flush();
                return base64Encoder.encode( bos.toByteArray() );
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( "Unable to encode array for indexing", e );
            }
            finally
            {
                try
                {
                    if ( out != null )
                    {
                        out.close();
                    }
                }
                catch ( IOException e )
                {
                    // ignore
                }
            }
        }
    }
}
