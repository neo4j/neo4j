/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionReader;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransactionWriterTest
{
    @Test
    public void shouldWriteTransaction() throws Exception
    {
        // given
        InMemoryLogBuffer buffer = new InMemoryLogBuffer();
        TransactionWriter writer = new TransactionWriter( buffer, 1, -1 );

        NodeRecord node = new NodeRecord( 0, -1, -1 );
        node.setLabelField( 0, Collections.<DynamicRecord>emptyList() );
        RelationshipRecord relationship = new RelationshipRecord( 0, 1, 1, 6 );

        // when
        writer.start( 1, 1, 0 );
        writer.create( node );
        writer.update( relationship );
        writer.delete( propertyRecordWithOneIntProperty( 3, 10, 45 ), new PropertyRecord( 3 ) );
        writer.prepare();
        writer.commit( false, 17 );
        writer.done();

        // then
        TransactionReader.Visitor visitor = visited( buffer );
        InOrder order = inOrder( visitor );
        order.verify( visitor ).visitStart( eq( 1 ), any( byte[].class ), eq( 1 ), eq( 1 ), anyLong() );
        order.verify( visitor ).visitUpdateNode( eq( 1 ), argThat( matchesRecord( node ) ) );
        order.verify( visitor ).visitUpdateRelationship( eq( 1 ), argThat( matchesRecord( relationship ) ) );
        order.verify( visitor ).visitDeleteProperty( eq( 1 ), eq( 3l ) );
        order.verify( visitor ).visitPrepare( eq( 1 ), anyLong() );
        order.verify( visitor ).visitCommit( eq( 1 ), eq( false ), eq( 17l ), anyLong() );
        order.verify( visitor ).visitDone( eq( 1 ) );
        verifyNoMoreInteractions( visitor );
    }

    private PropertyRecord propertyRecordWithOneIntProperty( long id, int keyId, int value )
    {
        PropertyRecord record = new PropertyRecord( id );
        record.setInUse( true );
        PropertyBlock block = new PropertyBlock();
        // Logic copied from PropertyStore#encodeValue
        block.setSingleBlock( keyId | (((long) PropertyType.INT.intValue()) << 24)
                | (value << 28) );
        record.addPropertyBlock( block );
        return record;
    }

    private static TransactionReader.Visitor visited( ReadableByteChannel source ) throws IOException
    {
        TransactionReader.Visitor visitor = mock( TransactionReader.Visitor.class );
        new TransactionReader().read( source, visitor );
        return visitor;
    }

    private final Map<Class<?>, Comparison> comparisons = new HashMap<Class<?>, Comparison>();

    @SuppressWarnings("unchecked")
    private <T extends AbstractBaseRecord> Matcher<T> matchesRecord( final T record )
    {
        final Comparison comparison = comparison( record.getClass() );
        return new TypeSafeMatcher<T>( (Class) record.getClass() )
        {
            @Override
            public boolean matchesSafely( T item )
            {
                return comparison.compare( record, item );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( record );
            }
        };
    }

    private Comparison comparison( Class<?> type )
    {
        Comparison comparison = comparisons.get( type );
        if ( comparison == null )
        {
            comparisons.put( type, comparison = new Comparison( type ) );
        }
        return comparison;
    }

    private static class Comparison
    {
        private final Collection<Field> fields = new ArrayList<Field>();

        Comparison( Class<?> type )
        {
            for ( Field field : type.getDeclaredFields() )
            {
                if ( field.getDeclaringClass() == type )
                {
                    field.setAccessible( true );
                    fields.add( field );
                }
            }
        }

        boolean compare( Object expected, Object actual )
        {
            try
            {
                for ( Field field : fields )
                {
                    if ( !equal( field.get( expected ), field.get( actual ) ) )
                    {
                        return false;
                    }
                }
                return true;
            }
            catch ( Exception failure )
            {
                return false;
            }
        }

        private static boolean equal( Object a, Object b )
        {
            return a == b || (a != null && (a.equals( b ) || (b != null && deepEquals( a, b ))));
        }

        private static boolean deepEquals( Object a, Object b )
        {
            if (a.getClass() == b.getClass() && a.getClass().isArray())
            {
                if ( a instanceof Object[] )
                {
                    return Arrays.deepEquals( (Object[]) a, (Object[]) b );
                }
                if ( a instanceof byte[] )
                {
                    return Arrays.equals( (byte[]) a, (byte[]) b );
                }
                if ( a instanceof char[] )
                {
                    return Arrays.equals( (char[]) a, (char[])b );
                }
            }
            return false;
        }
    }
}
