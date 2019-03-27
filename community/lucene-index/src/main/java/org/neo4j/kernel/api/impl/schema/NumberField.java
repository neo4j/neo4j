/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public class NumberField extends Field
{
    private static final IndexableFieldType TYPE = getType();

    private NumberField( String name, double value )
    {
        super( name, encode( value ), TYPE );
    }

    public static Field of( String name, double value )
    {
        return new NumberField( name, value );
//        return new DoublePoint( name, value );
    }

    private static IndexableFieldType getType()
    {
        FieldType type = new FieldType();
//        type.setDocValuesType( DocValuesType.NUMERIC ); ???
        type.setDimensions( 1, Double.BYTES );
        type.setIndexOptions( IndexOptions.DOCS ); // The entire point of this class, is to set IndexOptions.DOCS (DoublePoint uses IndexOptions.NONE).
        type.freeze();
        return type;
    }

    public static BytesRef encode( double value )
    {
        byte[] bytes = new byte[TYPE.pointNumBytes()];
        DoublePoint.encodeDimension( value, bytes, 0 );
        return new BytesRef( bytes );
    }

    public static double decode( byte[] value )
    {
        assert value.length == TYPE.pointNumBytes();
        return DoublePoint.decodeDimension( value, 0 );
    }

    public static Query newExactQuery( String field, double value )
    {
        return DoublePoint.newExactQuery( field, value );
    }

    public static Query newRangeQuery( String field, double min, double max )
    {
        return DoublePoint.newRangeQuery( field, min, max );
    }

    @Override
    public void setDoubleValue( double value )
    {
        fieldsData = encode( value );
    }

    @Override
    public String stringValue()
    {
        BytesRef fieldsData = (BytesRef) super.fieldsData;
        return String.valueOf( decode( fieldsData.bytes ) );
    }

    @Override
    public Number numericValue()
    {
        BytesRef fieldsData = (BytesRef) super.fieldsData;
        return decode( fieldsData.bytes );
    }
}
