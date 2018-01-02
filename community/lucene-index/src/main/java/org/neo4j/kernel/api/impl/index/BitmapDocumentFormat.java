/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import static java.lang.String.format;

public enum BitmapDocumentFormat
{
    _32( BitmapFormat._32 )
    {
        @Override
        protected NumericField setFieldValue( NumericField field, long bitmap )
        {
            assert (bitmap & 0xFFFFFFFF00000000L) == 0 :
                "Tried to store a bitmap as int, but which had values outside int limits";
            return field.setIntValue( (int) bitmap );
        }
    },
    _64( BitmapFormat._64 )
    {
        @Override
        protected NumericField setFieldValue( NumericField field, long bitmap )
        {
            return field.setLongValue( bitmap );
        }
    };

    public static final String RANGE = "range", LABEL = "label";
    private final BitmapFormat format;

    private BitmapDocumentFormat( BitmapFormat format )
    {
        this.format = format;
    }

    @Override
    public String toString()
    {
        return format( "%s{%s bit}", getClass().getSimpleName(), format );
    }

    public BitmapFormat bitmapFormat()
    {
        return format;
    }

    public long rangeOf( Document doc )
    {
        return Long.parseLong( doc.get( RANGE ) );
    }

    public long rangeOf( Fieldable field )
    {
        return Long.parseLong( field.stringValue() );
    }

    public long mapOf( Document doc, long labelId )
    {
        return bitmap( doc.getFieldable( label( labelId ) ) );
    }

    public Query labelQuery( long labelId )
    {
        return new TermQuery( new Term( LABEL, Long.toString( labelId ) ) );
    }

    public Query rangeQuery( long range )
    {
        return new TermQuery( new Term( RANGE, Long.toString( range) ) );
    }

    public Fieldable rangeField( long range )
    {
        // TODO: figure out what flags to set on the field
        Field field = new Field( RANGE, Long.toString( range ), Field.Store.YES, Field.Index.NOT_ANALYZED );
        field.setOmitNorms( true );
        field.setIndexOptions( FieldInfo.IndexOptions.DOCS_ONLY );
        return field;
    }

    public Fieldable labelField( long key, long bitmap )
    {
        // Label Fields are DOCUMENT ONLY (not indexed)
        // TODO: figure out what flags to set on the field
        NumericField field = new NumericField( label( key ), Field.Store.YES, false );
        field = setFieldValue( field, bitmap );
        field.setOmitNorms( true );
        field.setIndexOptions( FieldInfo.IndexOptions.DOCS_ONLY );
        return field;
    }

    protected abstract NumericField setFieldValue( NumericField field, long bitmap );

    public void addLabelField( Document document, long label, Bitmap bitmap )
    {
        document.add( labelField( label, bitmap ) );
        document.add( labelSearchField( label ) );
    }

    public Fieldable labelSearchField( long label )
    {
        // Label Search Fields are INDEX ONLY (not stored in the document)
        Field field = new Field( LABEL, Long.toString( label ), Field.Store.NO, Field.Index.NOT_ANALYZED );
        field.setOmitNorms( true );
        field.setIndexOptions( FieldInfo.IndexOptions.DOCS_ONLY );
        return field;
    }

    public Fieldable labelField( long key, Bitmap value )
    {
        return labelField( key, value.bitmap() );
    }

    String label( long key )
    {
        return Long.toString( key );
    }

    public long labelId( Fieldable field )
    {
        return Long.parseLong( field.name() );
    }

    public Term rangeTerm( long range )
    {
        return new Term( RANGE, Long.toString( range ) );
    }

    public Term rangeTerm( Document document )
    {
        return new Term( RANGE, document.get( RANGE ) );
    }

    public boolean isRangeField( Fieldable field )
    {
        String fieldName = field.name();
        return RANGE.equals( fieldName ) || LABEL.equals( fieldName );
    }

    public Bitmap readBitmap( Fieldable field )
    {
        return new Bitmap( bitmap( field ) );
    }

    private long bitmap( Fieldable field )
    {
        if ( field == null )
        {
            return 0;
        }
        if ( field instanceof NumericField )
        {
            return ((NumericField) field).getNumericValue().longValue();
        }
        throw new IllegalArgumentException( field + " is not a numeric field" );
    }
}
