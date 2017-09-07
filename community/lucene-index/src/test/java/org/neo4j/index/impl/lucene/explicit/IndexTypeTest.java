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
package org.neo4j.index.impl.lucene.explicit;


import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;

import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class IndexTypeTest
{

    private static final String STRING_TEST_FIELD = "testString";
    private static final String STRING_TEST_FIELD2 = "testString2";
    private static final String NUMERIC_TEST_FIELD = "testNumeric";
    private static final String NUMERIC_TEST_FIELD2 = "testNumeric2";

    @Parameterized.Parameter( 0 )
    public IndexType indexType;
    @Parameterized.Parameter( 1 )
    public int documentFieldsPerUserField;

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object> indexTypes()
    {
        Map<String,String> customIndexTypeConfig = MapUtil.stringMap( LuceneIndexImplementation.KEY_TYPE, "exact",
                LuceneIndexImplementation.KEY_TO_LOWER_CASE, "true" );
        return Arrays.asList( new Object[]{IndexType.EXACT, 2},
                new Object[]{IndexType.getIndexType( customIndexTypeConfig ), 3} );
    }

    @Test
    public void removeFromExactIndexedDocumentRetainCorrectNumberOfFields() throws Exception
    {
        Document document = new Document();
        indexType.addToDocument( document, STRING_TEST_FIELD, "value"  );
        indexType.addToDocument( document, STRING_TEST_FIELD2, "value2"  );
        indexType.addToDocument( document, NUMERIC_TEST_FIELD, 1  );
        indexType.addToDocument( document, NUMERIC_TEST_FIELD2, 2  );
        indexType.removeFromDocument( document, STRING_TEST_FIELD, null );
        assertEquals("Usual fields, doc values fields for user fields and housekeeping fields.",
                documentFieldsPerUserField * 3, document.getFields().size() );
        assertEquals("Two string fields with specified name expected.",
                2, getDocumentFields( document, STRING_TEST_FIELD2 ).length );
        assertEquals("Two numeric fields with specified name expected.",
                2, getDocumentFields( document, NUMERIC_TEST_FIELD ).length );
        assertEquals("Two numeric fields with specified name expected.",
                2, getDocumentFields( document, NUMERIC_TEST_FIELD2 ).length );
    }

    @Test
    public void removeFieldFromExactIndexedDocumentRetainCorrectNumberOfFields() throws Exception
    {
        Document document = new Document();
        indexType.addToDocument( document, STRING_TEST_FIELD, "value"  );
        indexType.addToDocument( document, STRING_TEST_FIELD2, "value2"  );
        indexType.addToDocument( document, NUMERIC_TEST_FIELD, 1  );
        indexType.addToDocument( document, NUMERIC_TEST_FIELD2, 2  );
        indexType.removeFieldsFromDocument( document, NUMERIC_TEST_FIELD, null );
        indexType.removeFieldsFromDocument( document, STRING_TEST_FIELD2, null );
        assertEquals("Usual fields, doc values fields for user fields and housekeeping fields.",
                documentFieldsPerUserField * 2, document.getFields().size() );
        assertEquals("Two string fields with specified name expected.",
                2, getDocumentFields( document, STRING_TEST_FIELD ).length );
        assertEquals("Two numeric fields with specified name expected.",
                2, getDocumentFields( document, NUMERIC_TEST_FIELD2 ).length );
    }

    private IndexableField[] getDocumentFields( Document document, String name )
    {
        return document.getFields( name );
    }
}
