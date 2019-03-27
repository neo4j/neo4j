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
package org.neo4j.kernel.api.impl.index;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.neo4j.helpers.collection.MapUtil;

public class IndexReaderStub extends LeafReader
{
    private Fields fields;
    private String[] elements = new String[0];
    private Function<String,NumericDocValues> ndvs;

    private static final FieldInfo DUMMY_FIELD_INFO =
            new FieldInfo( "id", 0, false, true, false, IndexOptions.DOCS,
                    DocValuesType.NONE, -1, Collections.emptyMap(), 1, 1, 8, true );

    public IndexReaderStub( final NumericDocValues ndv )
    {
        this.ndvs = s -> ndv;
    }

    public IndexReaderStub( Fields fields )
    {
        this.fields = fields;
    }

    public void setElements( String[] elements )
    {
        this.elements = elements;
    }

    @Override
    public CacheHelper getCoreCacheHelper()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Terms terms( String field )
    {
        if ( fields != null )
        {
            try
            {
                return fields.terms( field );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        return null;
    }

    @Override
    public NumericDocValues getNumericDocValues( String field )
    {
        return ndvs.apply( field );
    }

    @Override
    public BinaryDocValues getBinaryDocValues( String field )
    {
        return DocValues.emptyBinary();
    }

    @Override
    public SortedDocValues getSortedDocValues( String field )
    {
        return DocValues.emptySorted();
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues( String field )
    {
        return DocValues.emptySortedNumeric( elements.length );
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues( String field )
    {
        return DocValues.emptySortedSet();
    }

    @Override
    public NumericDocValues getNormValues( String field )
    {
        return DocValues.emptyNumeric();
    }

    @Override
    public FieldInfos getFieldInfos()
    {
        List<FieldInfo> infos = new ArrayList<>();
        int id = 0;
        for ( String field : fields )
        {
            infos.add( new FieldInfo( field, id++, true, false, false, IndexOptions.DOCS, DocValuesType.SORTED, 1, MapUtil.stringMap(), 1, 1, 8, false ) );
        }
        return new FieldInfos( infos.toArray( new FieldInfo[0] ) );
    }

    @Override
    public Bits getLiveDocs()
    {
        return new Bits()
        {
            @Override
            public boolean get( int index )
            {
                if ( index >= elements.length )
                {
                    throw new IllegalArgumentException( "Doc id out of range" );
                }
                return true;
            }

            @Override
            public int length()
            {
                return elements.length;
            }
        };
    }

    @Override
    public PointValues getPointValues( String field )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkIntegrity()
    {
    }

    @Override
    public LeafMetaData getMetaData()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Fields getTermVectors( int docID )
    {
        throw new RuntimeException( "Not yet implemented." );
    }

    @Override
    public int numDocs()
    {
        return elements.length;
    }

    @Override
    public int maxDoc()
    {
        return Math.max( maxValue(), elements.length) + 1;
    }

    @Override
    public void document( int docID, StoredFieldVisitor visitor ) throws IOException
    {
        visitor.stringField( DUMMY_FIELD_INFO, String.valueOf( docID ).getBytes( StandardCharsets.UTF_8 ) );
    }

    @Override
    protected void doClose()
    {
    }

    @Override
    public CacheHelper getReaderCacheHelper()
    {
        throw new UnsupportedOperationException();
    }

    private int maxValue()
    {
        return Arrays.stream( elements )
                .mapToInt( value ->  NumberUtils.toInt( value, 0 )).max().orElse( 0 );
    }
}
