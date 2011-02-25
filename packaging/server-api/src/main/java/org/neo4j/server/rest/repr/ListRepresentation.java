/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.repr;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.helpers.collection.PrefetchingIterator;

import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;

public final class ListRepresentation extends Representation
{
    private final Iterable<? extends Representation> content;

    public ListRepresentation( final String type, final Iterable<? extends Representation> content )
    {
        super( type );
        this.content = content;
    }

    public ListRepresentation( RepresentationType type,
            final Iterable<? extends Representation> content )
    {
        super( type );
        this.content = content;
    }

    @Override
    String serialize( RepresentationFormat format, URI baseUri, ExtensionInjector extensions )
    {
        ListWriter writer = format.serializeList( type );
        serialize( new ListSerializer( writer, baseUri, extensions ) );
        writer.done();
        return format.complete( writer );
    }

    void serialize( ListSerializer serializer )
    {
        for ( Representation repr : content )
        {
            repr.addTo( serializer );
        }
    }

    @Override
    void addTo( ListSerializer serializer )
    {
        serializer.addList( this );
    }

    @Override
    void putTo( MappingSerializer serializer, String key )
    {
        serializer.putList( key, this );
    }

    public static ListRepresentation strings( String... values )
    {
        return string( Arrays.asList( values ) );
    }

    public static ListRepresentation string( Iterable<String> values )
    {
        return new ListRepresentation( RepresentationType.STRING, new IterableWrapper<Representation, String>(
                values )
        {
            @Override
            protected Representation underlyingObjectToObject( String value )
            {
                return ValueRepresentation.string( value );
            }
        } );
    }

    public static ListRepresentation relationshipTypes( Iterable<RelationshipType> types )
    {
        return new ListRepresentation( RepresentationType.RELATIONSHIP_TYPE,
                new IterableWrapper<Representation, RelationshipType>( types )
                {
                    @Override
                    protected Representation underlyingObjectToObject( RelationshipType value )
                    {
                        return ValueRepresentation.relationshipType( value );
                    }
                } );
    }

    public static ListRepresentation numbers( final long... values )
    {
        return new ListRepresentation( RepresentationType.LONG, new Iterable<ValueRepresentation>()
        {
            @Override
            public Iterator<ValueRepresentation> iterator()
            {
                return new PrefetchingIterator<ValueRepresentation>()
                {
                    int pos = 0;

                    @Override
                    protected ValueRepresentation fetchNextOrNull()
                    {
                        if ( pos >= values.length ) return null;
                        return ValueRepresentation.number( values[pos++] );
                    }
                };
            }
        } );
    }

    public static ListRepresentation numbers( final double[] values )
    {
        return new ListRepresentation( RepresentationType.DOUBLE,
                new Iterable<ValueRepresentation>()
                {
                    @Override
                    public Iterator<ValueRepresentation> iterator()
                    {
                        return new PrefetchingIterator<ValueRepresentation>()
                        {
                            int pos = 0;

                            @Override
                            protected ValueRepresentation fetchNextOrNull()
                            {
                                if ( pos >= values.length ) return null;
                                return ValueRepresentation.number( values[pos++] );
                            }
                        };
                    }
                } );
    }
}
