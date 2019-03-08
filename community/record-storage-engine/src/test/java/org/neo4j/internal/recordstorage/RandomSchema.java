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
package org.neo4j.internal.recordstorage;

import java.util.Optional;
import java.util.SplittableRandom;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.common.EntityType;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.MultiTokenSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.storageengine.api.ConstraintRule;
import org.neo4j.storageengine.api.DefaultStorageIndexReference;
import org.neo4j.storageengine.api.StorageIndexReference;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueType;

import static org.neo4j.storageengine.api.ConstraintRule.constraintRule;

@SuppressWarnings( "WeakerAccess" ) // Keep accessibility high in case someone wants to extend this class in the future.
public class RandomSchema implements Supplier<SchemaRule>
{
    private final SplittableRandom rng;
    private final int maxPropId;
    private final int maxRelTypeId;
    private final int maxLabelId;
    private final int defaultLabelIdsArrayMaxLength;
    private final int defaultRelationshipTypeIdsArrayMaxLength;
    private final int defaultPropertyKeyIdsArrayMaxLength;
    private final RandomValues values;
    private final ValueType[] textTypes;

    public RandomSchema()
    {
        this( new SplittableRandom() );
    }

    public RandomSchema( SplittableRandom rng )
    {
        this.rng = rng;
        maxPropId = maxPropertyId();
        maxRelTypeId = maxRelationshipTypeId();
        maxLabelId = maxLabelId();
        defaultLabelIdsArrayMaxLength = defaultLabelIdsArrayMaxLength();
        defaultRelationshipTypeIdsArrayMaxLength = defaultRelationshipTypeIdsArrayMaxLength();
        defaultPropertyKeyIdsArrayMaxLength = defaultPropertyKeyIdsArrayMaxLength();
        values = RandomValues.create( rng, valuesConfiguration() );
        textTypes = RandomValues.typesOfGroup( ValueGroup.TEXT );
    }

    protected int maxPropertyId()
    {
        return (1 << StandardFormatSettings.PROPERTY_TOKEN_MAXIMUM_ID_BITS) - 1;
    }

    protected int maxRelationshipTypeId()
    {
        return (1 << StandardFormatSettings.RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS) - 1;
    }

    protected int maxLabelId()
    {
        return Integer.MAX_VALUE;
    }

    protected int defaultLabelIdsArrayMaxLength()
    {
        return 200;
    }

    protected int defaultRelationshipTypeIdsArrayMaxLength()
    {
        return 200;
    }

    protected int defaultPropertyKeyIdsArrayMaxLength()
    {
        return 300;
    }

    protected RandomValues.Default valuesConfiguration()
    {
        return new RandomValues.Default()
        {
            @Override
            public int stringMaxLength()
            {
                return 200;
            }

            @Override
            public int minCodePoint()
            {
                return super.minCodePoint() + 1; // Avoid null-bytes in our strings.
            }
        };
    }

    public Stream<SchemaRule> schemaRules()
    {
        return Stream.generate( this );
    }

    @Override
    public SchemaRule get()
    {
        return nextSchemaRule();
    }

    public SchemaRule nextSchemaRule()
    {
        if ( rng.nextBoolean() )
        {
            return nextIndex();
        }
        else
        {
            return nextConstraint();
        }
    }

    public StorageIndexReference nextIndex()
    {
        long ruleId = nextRuleIdForIndex();

        int choice = rng.nextInt( 9 );
        boolean isUnique = rng.nextBoolean();
        Long owningConstraint = rng.nextBoolean() ? existingConstraintId() : null;
        switch ( choice )
        {
        case 0: return new DefaultStorageIndexReference( nextNodeSchema(), isUnique, ruleId, owningConstraint );
        case 1: return new DefaultStorageIndexReference( nextNodeSchema(), nextName(), nextName(), ruleId, Optional.empty(), isUnique,
                owningConstraint, false );
        case 2: return new DefaultStorageIndexReference( nextNodeSchema(), nextName(), nextName(), ruleId, nextNameOpt(), isUnique, owningConstraint, false );
        case 3: return new DefaultStorageIndexReference( nextNodeMultiTokenSchema(), isUnique, ruleId, owningConstraint );
        case 4: return new DefaultStorageIndexReference( nextNodeMultiTokenSchema(), nextName(), nextName(), ruleId, Optional.empty(), isUnique,
                owningConstraint, false );
        case 5: return new DefaultStorageIndexReference( nextNodeMultiTokenSchema(), nextName(), nextName(), ruleId, nextNameOpt(), isUnique,
                owningConstraint, false );
        case 6: return new DefaultStorageIndexReference( nextRelationshipMultiTokenSchema(), isUnique, ruleId, owningConstraint );
        case 7: return new DefaultStorageIndexReference( nextRelationshipMultiTokenSchema(), nextName(), nextName(), ruleId, Optional.empty(), isUnique,
                owningConstraint, false );
        case 8: return new DefaultStorageIndexReference( nextRelationshipMultiTokenSchema(), nextName(), nextName(), ruleId, nextNameOpt(), isUnique,
                owningConstraint, false );
        default: throw new RuntimeException( "Bad index choice: " + choice );
        }
    }

    public Optional<String> nextNameOpt()
    {
        return Optional.of( nextName() );
    }

    public long nextRuleIdForIndex()
    {
        return nextRuleId();
    }

    public long existingConstraintId()
    {
        return nextRuleId();
    }

    public ConstraintRule nextConstraint()
    {
        long ruleId = nextRuleIdForConstraint();

        int choice = rng.nextInt( 12 );
        switch ( choice )
        {
        case 0: return constraintRule( ruleId, ConstraintDescriptorFactory.existsForSchema( nextRelationshipSchema() ) );
        case 1: return constraintRule( ruleId, ConstraintDescriptorFactory.existsForSchema( nextRelationshipSchema() ), nextName() );
        case 2: return constraintRule( ruleId, ConstraintDescriptorFactory.existsForSchema( nextNodeSchema() ) );
        case 3: return constraintRule( ruleId, ConstraintDescriptorFactory.existsForSchema( nextNodeSchema() ), nextName() );
        case 4: return constraintRule( ruleId, ConstraintDescriptorFactory.uniqueForSchema( nextNodeSchema() ) );
        case 5: return constraintRule( ruleId, ConstraintDescriptorFactory.uniqueForSchema( nextNodeSchema() ), existingIndexId() );
        case 6: return constraintRule( ruleId, ConstraintDescriptorFactory.uniqueForSchema( nextNodeSchema() ), nextName() );
        case 7: return constraintRule( ruleId, ConstraintDescriptorFactory.uniqueForSchema( nextNodeSchema() ), existingIndexId(), nextName() );
        case 8: return constraintRule( ruleId, ConstraintDescriptorFactory.nodeKeyForSchema( nextNodeSchema() ) );
        case 9: return constraintRule( ruleId, ConstraintDescriptorFactory.nodeKeyForSchema( nextNodeSchema() ), existingIndexId() );
        case 10: return constraintRule( ruleId, ConstraintDescriptorFactory.nodeKeyForSchema( nextNodeSchema() ), nextName() );
        case 11: return constraintRule( ruleId, ConstraintDescriptorFactory.nodeKeyForSchema( nextNodeSchema() ), existingIndexId(), nextName() );
        default: throw new RuntimeException( "Bad constraint choice: " + choice );
        }
    }

    public long nextRuleIdForConstraint()
    {
        return nextRuleId();
    }

    public long existingIndexId()
    {
        return nextRuleId();
    }

    public LabelSchemaDescriptor nextNodeSchema()
    {
        return SchemaDescriptorFactory.forLabel( nextLabelId(), nextPropertyKeyIdsArray() );
    }

    public RelationTypeSchemaDescriptor nextRelationshipSchema()
    {
        return SchemaDescriptorFactory.forRelType( nextRelationshipTypeId(), nextPropertyKeyIdsArray() );
    }

    public MultiTokenSchemaDescriptor nextNodeMultiTokenSchema()
    {
        return SchemaDescriptorFactory.multiToken( nextLabelIdsArray(), EntityType.NODE, nextPropertyKeyIdsArray() );
    }

    public MultiTokenSchemaDescriptor nextRelationshipMultiTokenSchema()
    {
        return SchemaDescriptorFactory.multiToken( nextRelationTypeIdsArray(), EntityType.RELATIONSHIP, nextPropertyKeyIdsArray() );
    }

    public int nextRuleId()
    {
        return rng.nextInt( Integer.MAX_VALUE );
    }

    public String nextName()
    {
        return ((TextValue) values.nextValueOfTypes( textTypes )).stringValue();
    }

    public int nextLabelId()
    {
        return rng.nextInt( maxLabelId );
    }

    public int nextRelationshipTypeId()
    {
        return rng.nextInt( maxRelTypeId );
    }

    public int[] nextLabelIdsArray()
    {
        return nextLabelIdsArray( defaultLabelIdsArrayMaxLength );
    }

    public int[] nextLabelIdsArray( int maxLength )
    {
        return rng.ints( rng.nextInt( maxLength + 1 ), 1, maxLabelId ).toArray();
    }

    public int[] nextRelationTypeIdsArray()
    {
        return nextRelationTypeIdsArray( defaultRelationshipTypeIdsArrayMaxLength );
    }

    public int[] nextRelationTypeIdsArray( int maxLength )
    {
        return rng.ints( rng.nextInt( maxLength + 1 ), 1, maxRelTypeId ).toArray();
    }

    public int[] nextPropertyKeyIdsArray()
    {
        return nextPropertyKeyIdsArray( defaultPropertyKeyIdsArrayMaxLength );
    }

    public int[] nextPropertyKeyIdsArray( int maxLength )
    {
        int propCount = rng.nextInt( 1, maxLength + 1 );
        return rng.ints( propCount, 1, maxPropId ).toArray();
    }
}
