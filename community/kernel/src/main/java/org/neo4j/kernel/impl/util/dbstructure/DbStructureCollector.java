/*
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
package org.neo4j.kernel.impl.util.dbstructure;

import org.neo4j.function.Function;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexDescriptor;

import java.util.*;

import static java.lang.String.format;

public class DbStructureCollector implements DbStructureVisitor
{
    private final TokenMap labels = new TokenMap( "label" );
    private final TokenMap propertyKeys = new TokenMap( "property key" );
    private final TokenMap relationshipTypes = new TokenMap( "relationship types" );
    private final IndexDescriptorMap regularIndices = new IndexDescriptorMap( "regular" );
    private final IndexDescriptorMap uniqueIndices = new IndexDescriptorMap( "unique" );
    private final Set<UniquenessConstraint> uniquenessConstraint = new HashSet<>();
    private final Map<Integer, Long> nodeCounts = new HashMap<>();
    private final Map<RelSpecifier, Long> relCounts = new HashMap<>();
    private long allNodesCount = -1l;

    public DbStructureLookup lookup()
    {
        return new DbStructureLookup()
        {
            @Override
            public Iterator<Pair<Integer, String>> labels()
            {
                return labels.iterator();
            }

            @Override
            public Iterator<Pair<Integer, String>> properties()
            {
                return propertyKeys.iterator();
            }

            @Override
            public Iterator<Pair<Integer, String>> relationshipTypes()
            {
                return relationshipTypes.iterator();
            }

            @Override
            public Iterator<Pair<String, String>> knownIndices()
            {
                return regularIndices.iterator();
            }

            @Override
            public Iterator<Pair<String, String>> knownUniqueIndices()
            {
                return uniqueIndices.iterator();
            }

            @Override
            public Iterator<Pair<String, String>> knownUniqueConstraints()
            {
                return Iterables.map( new Function<UniquenessConstraint, Pair<String, String>>()
                    {
                    @Override
                    public Pair<String, String> apply( UniquenessConstraint uniquenessConstraint ) throws RuntimeException
                    {
                        String label = labels.byIdOrFail( uniquenessConstraint.label() );
                        String propertyKey = propertyKeys.byIdOrFail( uniquenessConstraint.propertyKeyId() );
                        return Pair.of( label, propertyKey );
                    }
                }, uniquenessConstraint.iterator() );
            }

            @Override
            public long nodesWithLabelCardinality( int labelId )
            {
                Long result = labelId == -1 ? allNodesCount : nodeCounts.get( labelId );
                return result == null ? 0l : result;
            }

            @Override
            public long cardinalityByLabelsAndRelationshipType( int fromLabelId, int relTypeId, int toLabelId )
            {
                RelSpecifier specifier = new RelSpecifier( fromLabelId, relTypeId, toLabelId );
                Long result = relCounts.get( specifier );
                return result == null ? 0l : result;
            }

            @Override
            public double indexSelectivity( int labelId, int propertyKeyId )
            {
                Double result1 = regularIndices.getIndex( labelId, propertyKeyId );
                Double result2 = result1 == null ? uniqueIndices.getIndex( labelId, propertyKeyId ) : result1;
                return result2 == null ? Double.NaN : result2;
            }
        };
    }

    @Override
    public void visitLabel( int labelId, String labelName )
    {
        labels.putToken( labelId, labelName );
    }

    @Override
    public void visitPropertyKey( int propertyKeyId, String propertyKeyName )
    {
        propertyKeys.putToken( propertyKeyId, propertyKeyName );
    }

    @Override
    public void visitRelationshipType( int relTypeId, String relTypeName )
    {
        relationshipTypes.putToken( relTypeId, relTypeName );
    }

    @Override
    public void visitIndex( IndexDescriptor descriptor, String userDescription, double uniqueValuesPercentage )
    {
        regularIndices.putIndex( descriptor, userDescription, uniqueValuesPercentage );
    }

    @Override
    public void visitUniqueIndex( IndexDescriptor descriptor, String userDescription, double uniqueValuesPercentage )
    {
        uniqueIndices.putIndex( descriptor, userDescription, uniqueValuesPercentage );
    }

    @Override
    public void visitUniqueConstraint( UniquenessConstraint constraint, String userDescription )
    {
        if ( !uniquenessConstraint.add( constraint ) )
        {
            throw new IllegalArgumentException(
                    format( "Duplicate  unique constraint %s for %s", constraint, userDescription )
            );
        }
    }

    @Override
    public void visitAllNodesCount( long nodeCount )
    {
        if ( allNodesCount < 0 )
        {
            allNodesCount = nodeCount;
        }
        else
        {
            throw new IllegalStateException( "Already received node count" );
        }
    }

    @Override
    public void visitNodeCount( int labelId, String labelName, long nodeCount )
    {
        if ( nodeCounts.put( labelId, nodeCount ) != null )
        {
            throw new IllegalArgumentException(
                    format( "Duplicate node count %s for label with id % s", nodeCount, labelName )
            );
        }
    }

    @Override
    public void visitRelCount( int startLabelId, int relTypeId, int endLabelId, String relCountQuery, long relCount )
    {
        RelSpecifier specifier = new RelSpecifier( startLabelId, relTypeId, endLabelId );

        if ( relCounts.put( specifier, relCount ) != null )
        {
            throw new IllegalArgumentException(
                    format( "Duplicate rel count %s for relationship specifier %s (corresponding query: %s)", relCount,
                            specifier, relCountQuery )
            );
        }
    }

    private static class RelSpecifier
    {
        public final int fromLabelId;
        public final int relTypeId;
        public final int toLabelId;

        public RelSpecifier( int fromLabelId, int relTypeId, int toLabelId )
        {
            this.fromLabelId = fromLabelId;
            this.relTypeId = relTypeId;
            this.toLabelId = toLabelId;
        }

        @Override
        public String toString()
        {
            return format(
                "RelSpecifier{fromLabelId=%d, relTypeId=%d, toLabelId=%d}", fromLabelId, relTypeId, toLabelId
            );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }

            RelSpecifier that = (RelSpecifier) o;
            return fromLabelId == that.fromLabelId && relTypeId == that.relTypeId && toLabelId == that.toLabelId;
        }

        @Override
        public int hashCode()
        {
            int result = fromLabelId;
            result = 31 * result + relTypeId;
            result = 31 * result + toLabelId;
            return result;
        }
    }

    private class IndexDescriptorMap implements Iterable<Pair<String, String>>
    {
        private final String indexType;
        private final Map<IndexDescriptor, Double> indexMap = new HashMap<>();

        public IndexDescriptorMap( String indexType )
        {
            this.indexType = indexType;
        }

        public void putIndex( IndexDescriptor descriptor, String userDescription, double uniqueValuesPercentage )
        {
            if ( indexMap.containsKey( descriptor ) )
            {
                throw new IllegalArgumentException(
                        format( "Duplicate index descriptor %s for %s index %s", descriptor, indexType,
                                userDescription )
                );
            }

            indexMap.put( descriptor, uniqueValuesPercentage );
        }

        public Double getIndex( int labelId, int propertyKeyId )
        {
            return indexMap.get( new IndexDescriptor( labelId, propertyKeyId ) );
        }

        public Iterator<Pair<String, String>> iterator()
        {
            final Iterator<IndexDescriptor> iterator = indexMap.keySet().iterator();
            return new Iterator<Pair<String, String>>()
            {
                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                @Override
                public Pair<String, String> next()
                {
                    IndexDescriptor next = iterator.next();
                    String label = labels.byIdOrFail( next.getLabelId() );
                    String propertyKey = propertyKeys.byIdOrFail( next.getPropertyKeyId() );
                    return Pair.of( label, propertyKey );
                }

                @Override
                public void remove()
                {
                    iterator.remove();
                }
            };
        }
    }

    private static class TokenMap implements Iterable<Pair<Integer, String>>
    {
        private final String tokenType;
        private final Map<Integer, String> forward = new HashMap<>();
        private final Map<String, Integer> backward = new HashMap<>();

        public TokenMap( String tokenType )
        {
            this.tokenType = tokenType;
        }

        public String byIdOrFail( int token )
        {
            String result = forward.get( token );
            if ( result == null )
            {
                throw new IllegalArgumentException( format( "Didn't find %s token with id %s", tokenType, token ) );
            }
            return result;
        }

        public void putToken( int token, String name )
        {
            if ( forward.containsKey( token ) )
            {
                throw new IllegalArgumentException(
                        format( "Duplicate id %s for name %s in %s token map", token, name, tokenType )
                );
            }


            if ( backward.containsKey( name ) )
            {
                throw new IllegalArgumentException(
                        format( "Duplicate name %s for id %s in %s token map", name, token, tokenType )
                );
            }

            forward.put( token, name );
            backward.put( name, token );
        }

        @Override
        public Iterator<Pair<Integer, String>> iterator()
        {
            final Iterator<Map.Entry<Integer, String>> iterator = forward.entrySet().iterator();
            return new Iterator<Pair<Integer, String>>()
            {
                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext();
                }

                @Override
                public Pair<Integer, String> next()
                {
                    Map.Entry<Integer, String> next = iterator.next();
                    return Pair.of( next.getKey(), next.getValue() );
                }

                @Override
                public void remove()
                {
                    iterator.remove();
                }
            };
        }
    }
}
