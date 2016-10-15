/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.labelscan.storestrategy;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import org.neo4j.kernel.api.impl.labelscan.bitmaps.Bitmap;
import org.neo4j.kernel.api.impl.labelscan.bitmaps.BitmapFormat;

import static java.lang.String.format;

public enum BitmapDocumentFormat
{
    _32( BitmapFormat._32 )
    {
        @Override
        protected Field createLabelField( String label, long bitmap )
        {
            assert (bitmap & 0xFFFFFFFF00000000L) == 0 :
                "Tried to store a bitmap as int, but which had values outside int limits";
            return new NumericDocValuesField( label, bitmap );
        }

        @Override
        protected void addLabelFields( Document document, String label, long bitmap )
        {
            assert (bitmap & 0xFFFFFFFF00000000L) == 0 :
                    "Tried to store a bitmap as int, but which had values outside int limits";
            document.add( new NumericDocValuesField( label, bitmap ) );
            document.add( new IntField( label, (int) bitmap, Field.Store.YES ) );
        }
    },
    _64( BitmapFormat._64 )
    {
        @Override
        protected Field createLabelField( String label, long bitmap )
        {
            return new NumericDocValuesField( label, bitmap );
        }

        @Override
        protected void addLabelFields( Document document, String label, long bitmap )
        {
            document.add( new NumericDocValuesField( label, bitmap ) );
            document.add( new LongField( label, bitmap, Field.Store.YES ) );
        }
    };

    public static final String RANGE = "range", LABEL = "label";
    private final BitmapFormat format;

    BitmapDocumentFormat( BitmapFormat format )
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

    public long rangeOf( IndexableField field )
    {
        return Long.parseLong( field.stringValue() );
    }

    public Query labelQuery( int labelId )
    {
        return new TermQuery( new Term( LABEL, Long.toString( labelId ) ) );
    }

    /**
     * Builds a {@link Query} suitable for returning documents of nodes having all or any
     * (depending on {@code occur}) of the given {@code labelIds}.
     *
     * @param occur {@link Occur} to use in the query.
     * @param labelIds label ids to query for.
     * @return {@link Query} for searching for documents with (all or any) of the label ids.
     */
    public Query labelsQuery( Occur occur, int[] labelIds )
    {
        assert labelIds.length > 0;
        if ( labelIds.length == 1 )
        {
            return labelQuery( labelIds[0] );
        }

        BooleanQuery.Builder query = new BooleanQuery.Builder();
        for ( int labelId : labelIds )
        {
            query.add( labelQuery( labelId ), occur );
        }
        return query.build();
    }

    public Query rangeQuery( long range )
    {
        return new TermQuery( new Term( RANGE, Long.toString( range ) ) );
    }

    public IndexableField rangeField( long range )
    {
        return new StringField( RANGE, Long.toString( range ), Field.Store.YES );
    }

    public void addRangeValuesField( Document doc, long range )
    {
        doc.add( rangeField( range ) );
        doc.add( new NumericDocValuesField( RANGE, range ) );
    }

    public IndexableField labelField( long key, long bitmap )
    {
        return createLabelField( label( key ), bitmap );
    }

    protected abstract Field createLabelField( String label, long bitmap );

    protected abstract void addLabelFields( Document document, String label, long bitmap );

    public void addLabelAndSearchFields( Document document, long label, Bitmap bitmap )
    {
        addLabelFields( document, label( label ), bitmap.bitmap() );
        document.add( labelSearchField( label ) );
    }

    public IndexableField labelSearchField( long label )
    {
        return new StringField( LABEL, Long.toString( label ), Field.Store.YES );
    }

    String label( long key )
    {
        return Long.toString( key );
    }

    public long labelId( IndexableField field )
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

    public boolean isRangeOrLabelField( IndexableField field )
    {
        String fieldName = field.name();
        return RANGE.equals( fieldName ) || LABEL.equals( fieldName );
    }

    public boolean isRangeField( IndexableField field )
    {
        return RANGE.equals( field.name() );
    }

    public boolean isLabelBitmapField( IndexableField field )
    {
        return !isRangeOrLabelField( field );
    }

    public Bitmap readBitmap( IndexableField field )
    {
        return new Bitmap( bitmap( field ) );
    }

    private long bitmap( IndexableField field )
    {
        if ( field == null )
        {
            return 0;
        }
        Number numericValue = field.numericValue();
        if ( numericValue == null )
        {
            throw new IllegalArgumentException( field + " is not a numeric field" );
        }
        return numericValue.longValue();
    }
}
