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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.unsafe.impl.batchimport.InputIterator;

import static java.lang.Integer.max;

/**
 * Takes an {@link InputIterator} and splits up {@link InputRelationship relationships} by type.
 * Uses {@link InputCache} to populate (all except the first type) on first pass, then reading from the
 * cached relationships per type for all the other types.
 */
public class PerTypeRelationshipSplitter extends PrefetchingIterator<InputIterator<InputRelationship>>
{
    private static final String MINORITY_TYPE = "minority";
    private final Object[] allRelationshipTypes;
    private final InputIterator<InputRelationship> actual;
    private final Predicate<Object> minorityRelationshipTypes;
    private final ToIntFunction<Object> typeToId;
    private final InputCache inputCache;

    private int typeCursor;
    private boolean allMinority;

    public PerTypeRelationshipSplitter( InputIterator<InputRelationship> actual, Object[] allRelationshipTypes,
            Predicate<Object> minorityRelationshipTypes, ToIntFunction<Object> typeToId, InputCache inputCache )
    {
        this.actual = actual;
        this.allRelationshipTypes = allRelationshipTypes;
        this.minorityRelationshipTypes = minorityRelationshipTypes;
        this.typeToId = typeToId;
        this.inputCache = inputCache;
    }

    @Override
    protected InputIterator<InputRelationship> fetchNextOrNull()
    {
        while ( typeCursor < allRelationshipTypes.length )
        {
            Object type = allRelationshipTypes[typeCursor++];
            if ( typeCursor == 1 )
            {
                if ( minorityRelationshipTypes.test( type ) )
                {
                    allMinority = true;
                    return null;
                }

                // This is the first relationship type. If we're lucky and this is a new import
                // then this type will also represent the type the most relationship are of.
                // We'll basically return the actual iterator, but with a filter to only return
                // this type. The other relationships will be cached by type.
                return new FilteringAndPerTypeCachingInputIterator( actual, type );
            }

            // This isn't the first relationship type. The first pass cached relationships
            // per type on disk into InputCache. Simply get the correct one and return.
            if ( !minorityRelationshipTypes.test( type ) )
            {
                return inputCache.relationships( cacheSubType( type ), true/*delete after use*/ ).iterator();
            }
        }
        return null;
    }

    String cacheSubType( Object type )
    {
        return String.valueOf( typeToId.applyAsInt( type ) );
    }

    /**
     * @return the type currently being iterated over, e.g. the type that the {@link InputIterator} returned
     * from the most recent call to iterates over.
     */
    public Object currentType()
    {
        return allRelationshipTypes[typeCursor-1];
    }

    int highestTypeId()
    {
        int highest = 0;
        for( Object type : allRelationshipTypes )
        {
            highest = max( highest, typeToId.applyAsInt( type ) );
        }
        return highest;
    }

    public InputIterator<InputRelationship> getMinorityRelationships()
    {
        return allMinority ? actual : inputCache.relationships( MINORITY_TYPE, true ).iterator();
    }

    public class FilteringAndPerTypeCachingInputIterator extends InputIterator.Delegate<InputRelationship>
    {
        private final Object currentType;
        // index into this array is actual typeId, which may be 0 - 2^16-1.
        private final Receiver<InputRelationship[],IOException>[] receivers;
        private final Receiver<InputRelationship[],IOException> minorityReceiver;
        private final InputRelationship[] transport = new InputRelationship[1];

        @SuppressWarnings( "unchecked" )
        public FilteringAndPerTypeCachingInputIterator( InputIterator<InputRelationship> actual, Object currentType )
        {
            super( actual );
            this.currentType = currentType;
            this.receivers = new Receiver[highestTypeId()+1];

            try
            {
                for ( Object type : allRelationshipTypes )
                {
                    if ( type.equals( currentType ) )
                    {
                        // We're iterating over this type, let's not cache it. Also accounted for in the
                        // receivers array above, which is 1 less than number of types in total.
                        continue;
                    }

                    // Collect all types that are consider a minority into one set of relationships
                    if ( !minorityRelationshipTypes.test( type ) )
                    {
                        int typeId = typeToId.applyAsInt( type );
                        receivers[typeId] = inputCache.cacheRelationships( cacheSubType( type ) );
                    }
                }
                minorityReceiver = inputCache.cacheRelationships( MINORITY_TYPE );
            }
            catch ( IOException e )
            {
                throw new InputException( "Error creating a cacher", e );
            }
        }

        @Override
        protected InputRelationship fetchNextOrNull()
        {
            while ( true )
            {
                InputRelationship candidate = super.fetchNextOrNull();
                if ( candidate == null )
                {
                    // No more relationships
                    return null;
                }

                Object type = candidate.typeAsObject();
                if ( type.equals( currentType ) )
                {
                    // This is a relationship of the requested type
                    return candidate;
                }

                // This is a relationships of a different type, cache it
                try
                {
                    Receiver<InputRelationship[],IOException> receiver = minorityRelationshipTypes.test( type ) ?
                            minorityReceiver : receivers[typeToId.applyAsInt( type )];
                    transport[0] = candidate;
                    receiver.receive( transport );
                }
                catch ( IOException e )
                {
                    throw new InputException( "Error caching relationship " + candidate, e );
                }
            }
        }

        @Override
        public void close()
        {
            try
            {
                for ( Receiver<InputRelationship[],IOException> receiver : receivers )
                {
                    if ( receiver != null )
                    {
                        receiver.close();
                    }
                }
                minorityReceiver.close();
            }
            catch ( IOException e )
            {
                throw new InputException( "Error closing cacher", e );
            }

            // This will delegate to the actual iterator and so close the external input iterator
            super.close();
        }
    }
}
