/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.Arrays;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.hashing.HashFunction;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

public class IndexDefinitionImpl implements IndexDefinition
{
    private final InternalSchemaActions actions;

    private final Label[] labels;
    private final RelationshipType[] relTypes;
    private final String[] propertyKeys;
    private final boolean constraintIndex;

    public IndexDefinitionImpl( InternalSchemaActions actions, Label label, String[] propertyKeys, boolean constraintIndex ) // TODO remove this constructor
    {
        this( actions, new Label[]{label}, null, propertyKeys, constraintIndex );
    }

    public IndexDefinitionImpl( InternalSchemaActions actions, Label[] labels, RelationshipType[] relTypes, String[] propertyKeys, boolean constraintIndex )
    {
        this.actions = actions;
        this.labels = labels;
        this.relTypes = relTypes;
        this.propertyKeys = propertyKeys;
        this.constraintIndex = constraintIndex;

        assertInUnterminatedTransaction();
    }

    @Override
    public Label getLabel()
    {
        assertInUnterminatedTransaction();
        assertIsNodeIndex();
        if ( labels.length > 1 )
        {
            throw new IllegalStateException( "This is a multi-token index, which has more than one label. Call the getLabels() method instead." );
        }
        return labels[0];
    }

    @Override
    public Iterable<Label> getLabels()
    {
        assertInUnterminatedTransaction();
        assertIsNodeIndex();
        return Arrays.asList( labels );
    }

    @Override
    public RelationshipType getRelationshipType()
    {
        assertInUnterminatedTransaction();
        assertIsRelationshipIndex();
        if ( relTypes.length > 1 )
        {
            throw new IllegalStateException(
                    "This is a multi-token index, which has more than one relationship type. " + "Call the getRelationshipTypes() method instead." );
        }
        return relTypes[0];
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        assertInUnterminatedTransaction();
        assertIsRelationshipIndex();
        return Arrays.asList( relTypes );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        assertInUnterminatedTransaction();
        return asList( propertyKeys );
    }

    /**
     * Returns the inner array of property keys in this index definition.
     * <p>
     * This array <em><strong>must not</strong></em> be modified, since the index definition is supposed to be
     * immutable.
     *
     * @return The array of property keys.
     */
    String[] getPropertyKeysArrayShared()
    {
        assertInUnterminatedTransaction();
        return propertyKeys;
    }

    @Override
    public void drop()
    {
        try
        {
            actions.dropIndexDefinitions( this );
        }
        catch ( ConstraintViolationException e )
        {
            if ( this.isConstraintIndex() )
            {
                throw new IllegalStateException( "Constraint indexes cannot be dropped directly, " + "instead drop the owning uniqueness constraint.", e );
            }
            throw e;
        }
    }

    @Override
    public boolean isConstraintIndex()
    {
        assertInUnterminatedTransaction();
        return constraintIndex;
    }

    @Override
    public boolean isNodeIndex()
    {
        assertInUnterminatedTransaction();
        return internalIsNodeIndex();
    }

    private boolean internalIsNodeIndex()
    {
        return labels != null;
    }

    @Override
    public boolean isRelationshipIndex()
    {
        assertInUnterminatedTransaction();
        return relTypes != null;
    }

    @Override
    public boolean isMultiTokenIndex()
    {
        assertInUnterminatedTransaction();
        return internalIsNodeIndex() ? labels.length > 1 : relTypes.length > 1;
    }

    @Override
    public boolean isCompositeIndex()
    {
        assertInUnterminatedTransaction();
        return propertyKeys.length > 1;
    }

    @Override
    public int hashCode()
    {
        HashFunction hf = HashFunction.incrementalXXH64();
        long hash = hf.initialise( 31 );
        if ( internalIsNodeIndex() )
        {
            hash = hf.update( hash, 42 ); // labels-array specific discriminator.
            hash = hf.update( hash, labels.length );
            for ( Label label : labels )
            {
                hash = hf.update( hash, label.name().hashCode() );
            }
        }
        if ( relTypes != null )
        {
            hash = hf.update( hash, 24 ); // relTypes-array specific discriminator.
            hash = hf.update( hash, relTypes.length );
            for ( RelationshipType relType : relTypes )
            {
                hash = hf.update( hash, relType.name().hashCode() );
            }
        }
        hash = hf.update( hash, propertyKeys.length );
        for ( String propertyKey : propertyKeys )
        {
            hash = hf.update( hash, propertyKey.hashCode() );
        }
        return hf.toInt( hash );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        IndexDefinitionImpl other = (IndexDefinitionImpl) obj;
        if ( internalIsNodeIndex() )
        {
            if ( other.labels == null )
            {
                return false;
            }
            if ( labels.length != other.labels.length )
            {
                return false;
            }
            for ( int i = 0; i < labels.length; i++ )
            {
                if ( !labels[i].name().equals( other.labels[i].name() ) )
                {
                    return false;
                }
            }
        }
        if ( relTypes != null )
        {
            if ( other.relTypes == null )
            {
                return false;
            }
            if ( relTypes.length != other.relTypes.length )
            {
                return false;
            }
            for ( int i = 0; i < relTypes.length; i++ )
            {
                if ( !relTypes[i].name().equals( other.relTypes[i].name() ) )
                {
                    return false;
                }
            }
        }
        return Arrays.equals( propertyKeys, other.propertyKeys );
    }

    @Override
    public String toString()
    {
        String entityTokenType;
        String entityTokens;
        if ( internalIsNodeIndex() )
        {
            entityTokenType = labels.length > 1 ? "labels" : "label";
            entityTokens = Arrays.stream( labels ).map( Label::name ).collect( joining( "," ) );
        }
        else
        {
            entityTokenType = relTypes.length > 1 ? "relationship types" : "relationship type";
            entityTokens = Arrays.stream( relTypes ).map( RelationshipType::name ).collect( joining( "," ) );
        }
        return "IndexDefinition[" + entityTokenType + ":" + entityTokens + " on:" + String.join( ",", propertyKeys ) + "]";
    }

    private void assertInUnterminatedTransaction()
    {
        actions.assertInOpenTransaction();
    }

    private void assertIsNodeIndex()
    {
        if ( !isNodeIndex() )
        {
            throw new IllegalStateException( "This is not a node index." );
        }
    }

    private void assertIsRelationshipIndex()
    {
        if ( !isRelationshipIndex() )
        {
            throw new IllegalStateException( "This is not a relationship index." );
        }
    }
}
