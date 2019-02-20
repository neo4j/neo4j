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
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.MultiTokenSchemaDescriptor;
import org.neo4j.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatSettings;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueType;

import static org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory.forSchema;
import static org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory.uniqueForSchema;
import static org.neo4j.kernel.impl.store.record.ConstraintRule.constraintRule;

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

    public StoreIndexDescriptor nextIndex()
    {
        long ruleId = nextRuleIdForIndex();

        int choice = rng.nextInt( 48 );
        switch ( choice )
        {
        case 0: return forSchema( nextNodeSchema() ).withId( ruleId );
        case 1: return forSchema( nextNodeSchema() ).withIds( ruleId, existingConstraintId() );
        case 2: return forSchema( nextNodeSchema(), nextIndexProvider() ).withId( ruleId );
        case 3: return forSchema( nextNodeSchema(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 4: return forSchema( nextNodeSchema(), Optional.empty(), nextIndexProvider() ).withId( ruleId );
        case 5: return forSchema( nextNodeSchema(), Optional.empty(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 6: return forSchema( nextNodeSchema(), nextNameOpt(), nextIndexProvider() ).withId( ruleId );
        case 7: return forSchema( nextNodeSchema(), nextNameOpt(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 8: return forSchema( nextNodeMultiTokenSchema() ).withId( ruleId );
        case 9: return forSchema( nextNodeMultiTokenSchema() ).withIds( ruleId, existingConstraintId() );
        case 10: return forSchema( nextNodeMultiTokenSchema(), nextIndexProvider() ).withId( ruleId );
        case 11: return forSchema( nextNodeMultiTokenSchema(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 12: return forSchema( nextNodeMultiTokenSchema(), Optional.empty(), nextIndexProvider() ).withId( ruleId );
        case 13: return forSchema( nextNodeMultiTokenSchema(), Optional.empty(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 14: return forSchema( nextNodeMultiTokenSchema(), nextNameOpt(), nextIndexProvider() ).withId( ruleId );
        case 15: return forSchema( nextNodeMultiTokenSchema(), nextNameOpt(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 16: return forSchema( nextRelationshipMultiTokenSchema() ).withId( ruleId );
        case 17: return forSchema( nextRelationshipMultiTokenSchema() ).withIds( ruleId, existingConstraintId() );
        case 18: return forSchema( nextRelationshipMultiTokenSchema(), nextIndexProvider() ).withId( ruleId );
        case 19: return forSchema( nextRelationshipMultiTokenSchema(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 20: return forSchema( nextRelationshipMultiTokenSchema(), Optional.empty(), nextIndexProvider() ).withId( ruleId );
        case 21: return forSchema( nextRelationshipMultiTokenSchema(), Optional.empty(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 22: return forSchema( nextRelationshipMultiTokenSchema(), nextNameOpt(), nextIndexProvider() ).withId( ruleId );
        case 23: return forSchema( nextRelationshipMultiTokenSchema(), nextNameOpt(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 24: return uniqueForSchema( nextNodeSchema() ).withId( ruleId );
        case 25: return uniqueForSchema( nextNodeSchema() ).withIds( ruleId, existingConstraintId() );
        case 26: return uniqueForSchema( nextNodeSchema(), nextIndexProvider() ).withId( ruleId );
        case 27: return uniqueForSchema( nextNodeSchema(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 28: return uniqueForSchema( nextNodeSchema(), Optional.empty(), nextIndexProvider() ).withId( ruleId );
        case 29: return uniqueForSchema( nextNodeSchema(), Optional.empty(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 30: return uniqueForSchema( nextNodeSchema(), nextNameOpt(), nextIndexProvider() ).withId( ruleId );
        case 31: return uniqueForSchema( nextNodeSchema(), nextNameOpt(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 32: return uniqueForSchema( nextNodeMultiTokenSchema() ).withId( ruleId );
        case 33: return uniqueForSchema( nextNodeMultiTokenSchema() ).withIds( ruleId, existingConstraintId() );
        case 34: return uniqueForSchema( nextNodeMultiTokenSchema(), nextIndexProvider() ).withId( ruleId );
        case 35: return uniqueForSchema( nextNodeMultiTokenSchema(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 36: return uniqueForSchema( nextNodeMultiTokenSchema(), Optional.empty(), nextIndexProvider() ).withId( ruleId );
        case 37: return uniqueForSchema( nextNodeMultiTokenSchema(), Optional.empty(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 38: return uniqueForSchema( nextNodeMultiTokenSchema(), nextNameOpt(), nextIndexProvider() ).withId( ruleId );
        case 39: return uniqueForSchema( nextNodeMultiTokenSchema(), nextNameOpt(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 40: return uniqueForSchema( nextRelationshipMultiTokenSchema() ).withId( ruleId );
        case 41: return uniqueForSchema( nextRelationshipMultiTokenSchema() ).withIds( ruleId, existingConstraintId() );
        case 42: return uniqueForSchema( nextRelationshipMultiTokenSchema(), nextIndexProvider() ).withId( ruleId );
        case 43: return uniqueForSchema( nextRelationshipMultiTokenSchema(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 44: return uniqueForSchema( nextRelationshipMultiTokenSchema(), Optional.empty(), nextIndexProvider() ).withId( ruleId );
        case 45: return uniqueForSchema( nextRelationshipMultiTokenSchema(), Optional.empty(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
        case 46: return uniqueForSchema( nextRelationshipMultiTokenSchema(), nextNameOpt(), nextIndexProvider() ).withId( ruleId );
        case 47: return uniqueForSchema( nextRelationshipMultiTokenSchema(), nextNameOpt(), nextIndexProvider() ).withIds( ruleId, existingConstraintId() );
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

    public IndexProviderDescriptor nextIndexProvider()
    {
        return new IndexProviderDescriptor( nextName(), nextName() );
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
