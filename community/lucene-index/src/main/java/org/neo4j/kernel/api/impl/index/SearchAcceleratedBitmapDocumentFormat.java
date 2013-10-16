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
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.neo4j.kernel.api.impl.index.bitmaps.Bitmap;
import org.neo4j.kernel.api.impl.index.bitmaps.BitmapFormat;

public class SearchAcceleratedBitmapDocumentFormat extends BitmapDocumentFormat
{
    static final String LABEL = "label";

    public SearchAcceleratedBitmapDocumentFormat( BitmapFormat format )
    {
        super( format );
    }

    @Override
    public Query labelQuery( long labelId )
    {
        return new TermQuery( new Term( LABEL, Long.toString( labelId ) ) );
    }

    @Override
    public void addLabelField( Document document, long label, Bitmap bitmap )
    {
        document.add( labelField( label, bitmap ) );
        document.add( labelSearchField( label ) );
    }

    private Fieldable labelSearchField( long label )
    {
        // Label Search Fields are INDEX ONLY (not stored in the document)
        Field field = new Field( LABEL, Long.toString( label ), Field.Store.NO, Field.Index.NOT_ANALYZED );
        field.setOmitNorms( true );
        field.setIndexOptions( FieldInfo.IndexOptions.DOCS_ONLY );
        return field;
    }

    @Override(/*to set the 'index' flag to false*/)
    public Fieldable labelField( long key, long bitmap )
    {
        // Label Fields are DOCUMENT ONLY (not indexed)
        // TODO: figure out what flags to set on the field
        NumericField field = new NumericField( label( key ), Field.Store.YES, false ).setLongValue( bitmap );
        field.setOmitNorms( true );
        field.setIndexOptions( FieldInfo.IndexOptions.DOCS_ONLY );
        return field;
    }
}
