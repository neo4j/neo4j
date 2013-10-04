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

import org.neo4j.kernel.api.impl.index.bitmaps.Bitmap;
import org.neo4j.kernel.api.impl.index.bitmaps.BitmapFormat;

import static org.apache.lucene.search.NumericRangeQuery.newLongRange;

public class BitmapDocumentFormat
{
    public static final String RANGE = "range";
    private final BitmapFormat format;

    protected BitmapDocumentFormat( BitmapFormat format )
    {
        this.format = format;
    }

    public BitmapFormat bitmapFormat()
    {
        return format;
    }

    public long rangeOf( Document doc )
    {
        return Long.parseLong( doc.get( RANGE ) );
        //Fieldable range = doc.getFieldable( RANGE );
        //if ( range instanceof NumericField )
        //{
        //    return  ((NumericField) range).getNumericValue().longValue();
        //}
        //throw new IllegalArgumentException( "Document does not have a numeric '" + RANGE + "' field." );
    }

    public long mapOf( Document doc, long labelId )
    {
        return bitmap( doc.getFieldable( label( labelId ) ) );
    }

    public Query labelQuery( long labelId )
    {
        // find all documents with the label field - regardless of value
        return newLongRange( label( labelId ), Long.MIN_VALUE, Long.MAX_VALUE, true, true );
    }

    public Fieldable rangeField( long range )
    {
        // TODO: figure out what flags to set on the field
        Field field = new Field( "range", Long.toString( range ), Field.Store.YES, Field.Index.NOT_ANALYZED );
        //NumericField field = new NumericField( RANGE, Field.Store.YES, true ).setLongValue( range );
        field.setOmitNorms( true );
        field.setIndexOptions( FieldInfo.IndexOptions.DOCS_ONLY );
        return field;
    }

    public Fieldable labelField( long key, long bitmap )
    {
        // TODO: figure out what flags to set on the field
        NumericField field = new NumericField( label( key ), Field.Store.YES, true ).setLongValue( bitmap );
        field.setOmitNorms( true );
        field.setIndexOptions( FieldInfo.IndexOptions.DOCS_ONLY );
        return field;
    }

    public Fieldable labelField( long key, Bitmap value )
    {
        return labelField( key, value.bitmap() );
    }

    private String label( long key )
    {
        return Long.toString( key );
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
        return RANGE.equals( field.name() );
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
