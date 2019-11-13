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
package org.neo4j.procedure.builtin;

import org.eclipse.collections.api.tuple.Pair;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import java.util.StringJoiner;
import java.util.function.Function;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

public final class SchemaStatementProcedure
{

    private static final String CREATE_UNIQUE_PROPERTY_CONSTRAINT = "CALL db.createUniquePropertyConstraint( '%s', %s, %s, '%s', %s )";
    private static final String CREATE_NODE_KEY_CONSTRAINT = "CALL db.createNodeKey( '%s', %s, %s, '%s', %s )";
    private static final String CREATE_NODE_EXISTENCE_CONSTRAINT = "CREATE CONSTRAINT `%s` ON (a:`%s`) ASSERT exists(a.`%s`)";
    private static final String CREATE_RELATIONSHIP_EXISTENCE_CONSTRAINT = "CREATE CONSTRAINT `%s` ON ()-[a:`%s`]-() ASSERT exists(a.`%s`)";
    private static final String CREATE_BTREE_INDEX = "CALL db.createIndex('%s', %s, %s, '%s', %s)";
    private static final String CREATE_NODE_FULLTEXT_INDEX = "CALL db.index.fulltext.createNodeIndex('%s', %s, %s, %s)";
    private static final String CREATE_RELATIONSHIP_FULLTEXT_INDEX = "CALL db.index.fulltext.createRelationshipIndex('%s', %s, %s, %s)";
    private static final String DROP_CONSTRAINT = "DROP CONSTRAINT `%s`";
    private static final String DROP_INDEX = "DROP INDEX `%s`";
    private static final String SINGLE_CONFIG = "`%s`: %s";

    private SchemaStatementProcedure()
    {
    }

    static Collection<BuiltInProcedures.SchemaStatementResult> createSchemaStatementResults( SchemaReadCore schemaRead, TokenRead tokenRead )
            throws ProcedureException
    {
        Map<String,BuiltInProcedures.SchemaStatementResult> schemaStatements = new HashMap<>();

        // Indexes
        // If index is unique the assumption is that it is backing a constraint and will not be included.
        final Iterator<IndexDescriptor> allIndexes = schemaRead.indexesGetAll();
        while ( allIndexes.hasNext() )
        {
            final IndexDescriptor index = allIndexes.next();
            if ( includeIndex( schemaRead, index ) )
            {
                final String name = index.getName();
                String type = SchemaRuleType.INDEX.name();
                final String createStatement = createStatement( tokenRead, index );
                final String dropStatement = dropStatement( index );
                schemaStatements.put( name, new BuiltInProcedures.SchemaStatementResult( name, type, createStatement, dropStatement ) );
            }
        }

        // Constraints
        Iterator<ConstraintDescriptor> allConstraints = schemaRead.constraintsGetAll();
        while ( allConstraints.hasNext() )
        {
            ConstraintDescriptor constraint = allConstraints.next();
            if ( includeConstraint( schemaRead, constraint ) )
            {
                String name = constraint.getName();
                String type = SchemaRuleType.CONSTRAINT.name();
                String createStatement = createStatement( schemaRead::indexGetForName, tokenRead, constraint );
                String dropStatement = dropStatement( constraint );
                schemaStatements.put( name, new BuiltInProcedures.SchemaStatementResult( name, type, createStatement, dropStatement ) );
            }
        }

        return schemaStatements.values();
    }

    private static boolean includeConstraint( SchemaReadCore schemaRead, ConstraintDescriptor constraint )
    {
        // If constraint is index backed constraint following rules apply
        // - Constraint must own index
        // - Owned index must exist
        // - Owned index must share name with constraint
        // - Owned index must be online
        // - Owned index must have this constraint as owning constraint
        if ( constraint.isIndexBackedConstraint() )
        {
            IndexBackedConstraintDescriptor indexBackedConstraint = constraint.asIndexBackedConstraint();
            if ( indexBackedConstraint.hasOwnedIndexId() )
            {
                IndexDescriptor backingIndex = schemaRead.indexGetForName( constraint.getName() );
                if ( backingIndex.getId() == indexBackedConstraint.ownedIndexId() )
                {
                    try
                    {
                        InternalIndexState internalIndexState = schemaRead.indexGetState( backingIndex );
                        OptionalLong owningConstraintId = backingIndex.getOwningConstraintId();
                        return internalIndexState == InternalIndexState.ONLINE && owningConstraintId.orElse( -1 ) == constraint.getId();
                    }
                    catch ( IndexNotFoundKernelException e )
                    {
                        return false;
                    }
                }
            }
            return false;
        }
        return true;
    }

    private static boolean includeIndex( SchemaReadCore schemaRead, IndexDescriptor index )
    {
        try
        {
            InternalIndexState indexState = schemaRead.indexGetState( index );
            return indexState == InternalIndexState.ONLINE && !index.isUnique();
        }
        catch ( IndexNotFoundKernelException e )
        {
            return false;
        }
    }

    public static String createStatement( Function<String,IndexDescriptor> indexLookup, TokenRead tokenRead, ConstraintDescriptor constraint )
            throws ProcedureException
    {
        try
        {
            String name = constraint.getName();
            if ( constraint.isIndexBackedConstraint() )
            {
                final String labelsOrRelTypes = labelsOrRelTypesAsStringArray( tokenRead, constraint.schema() );
                final String properties = propertiesAsStringArray( tokenRead, constraint.schema() );
                IndexDescriptor backingIndex = indexLookup.apply( name );
                String providerName = backingIndex.getIndexProvider().name();
                String config = btreeConfigAsString( backingIndex );
                if ( constraint.isUniquenessConstraint() )
                {
                    return format( CREATE_UNIQUE_PROPERTY_CONSTRAINT,
                            name, labelsOrRelTypes, properties, providerName, config );
                }
                if ( constraint.isNodeKeyConstraint() )
                {
                    return format( CREATE_NODE_KEY_CONSTRAINT,
                            name, labelsOrRelTypes, properties, providerName, config );
                }
            }
            if ( constraint.isNodePropertyExistenceConstraint() )
            {
                // "create CONSTRAINT ON (a:A) ASSERT exists(a.p)"
                int labelId = constraint.schema().getLabelId();
                String label = tokenRead.nodeLabelName( labelId );
                int propertyId = constraint.schema().getPropertyId();
                String property = tokenRead.propertyKeyName( propertyId );
                return format( CREATE_NODE_EXISTENCE_CONSTRAINT,
                        name, label, property );
            }
            if ( constraint.isRelationshipPropertyExistenceConstraint() )
            {
                // "create CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)"
                int relationshipTypeId = constraint.schema().getRelTypeId();
                String relationshipType = tokenRead.relationshipTypeName( relationshipTypeId );
                int propertyId = constraint.schema().getPropertyId();
                String property = tokenRead.propertyKeyName( propertyId );
                return format( CREATE_RELATIONSHIP_EXISTENCE_CONSTRAINT,
                        name, relationshipType, property );
            }
            throw new IllegalArgumentException( "Did not recognize constraint type " + constraint );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.General.UnknownError, e, "Failed to re-create create statement." );
        }
    }

    private static String dropStatement( ConstraintDescriptor constraint )
    {
        return format( DROP_CONSTRAINT, constraint.getName() );
    }

    public static String createStatement( TokenRead tokenRead, IndexDescriptor indexDescriptor ) throws ProcedureException
    {
        try
        {
            final String name = indexDescriptor.getName();
            final String labelsOrRelTypes = labelsOrRelTypesAsStringArray( tokenRead, indexDescriptor.schema() );
            final String properties = propertiesAsStringArray( tokenRead, indexDescriptor.schema() );
            switch ( indexDescriptor.getIndexType() )
            {
            case BTREE:
                String btreeConfig = btreeConfigAsString( indexDescriptor );
                final String providerName = indexDescriptor.getIndexProvider().name();
                return format( CREATE_BTREE_INDEX,
                        name, labelsOrRelTypes, properties, providerName, btreeConfig );
            case FULLTEXT:
                String fulltextConfig = fulltextConfigAsString( indexDescriptor );
                switch ( indexDescriptor.schema().entityType() )
                {
                case NODE:
                    return format( CREATE_NODE_FULLTEXT_INDEX,
                            name, labelsOrRelTypes, properties, fulltextConfig );
                case RELATIONSHIP:
                    return format( CREATE_RELATIONSHIP_FULLTEXT_INDEX,
                            name, labelsOrRelTypes, properties, fulltextConfig );
                default:
                    throw new IllegalArgumentException( "Did not recognize entity type " + indexDescriptor.schema().entityType() );
                }
            default:
                throw new IllegalArgumentException( "Did not recognize index type " + indexDescriptor.getIndexType() );
            }
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.General.UnknownError, e, "Failed to re-create create statement." );
        }
    }

    private static String btreeConfigAsString( IndexDescriptor indexDescriptor )
    {
        final IndexConfig indexConfig = indexDescriptor.getIndexConfig();
        StringJoiner configString = configStringJoiner();
        for ( Pair<String,Value> entry : indexConfig.entries() )
        {
            String singleConfig = format( SINGLE_CONFIG, entry.getOne(), btreeConfigValueAsString( entry.getTwo() ) );
            configString.add( singleConfig );
        }
        return configString.toString();
    }

    private static String btreeConfigValueAsString( Value configValue )
    {
        if ( configValue instanceof DoubleArray )
        {
            final DoubleArray doubleArray = (DoubleArray) configValue;
            return Arrays.toString( doubleArray.asObjectCopy() );
        }
        if ( configValue instanceof IntValue )
        {
            final IntValue intValue = (IntValue) configValue;
            return "" + intValue.value();
        }
        if ( configValue instanceof BooleanValue )
        {
            final BooleanValue booleanValue = (BooleanValue) configValue;
            return "" + booleanValue.booleanValue();
        }
        if ( configValue instanceof StringValue )
        {
            final StringValue stringValue = (StringValue) configValue;
            return "'" + stringValue.stringValue() + "'";
        }
        throw new IllegalArgumentException( "Could not convert config value '" + configValue + "' to config string." );
    }

    private static String fulltextConfigAsString( IndexDescriptor indexDescriptor )
    {
        final IndexConfig indexConfig = indexDescriptor.getIndexConfig();
        StringJoiner configString = configStringJoiner();
        for ( Pair<String,Value> entry : indexConfig.entries() )
        {
            String key = entry.getOne();
            String singleConfig = format( SINGLE_CONFIG, key, fulltextConfigValueAsString( entry.getTwo() ) );
            configString.add( singleConfig );
        }
        return configString.toString();
    }

    private static String fulltextConfigValueAsString( Value configValue )
    {
        if ( configValue instanceof BooleanValue )
        {
            final BooleanValue booleanValue = (BooleanValue) configValue;
            return "'" + booleanValue.booleanValue() + "'";
        }
        if ( configValue instanceof StringValue )
        {
            final StringValue stringValue = (StringValue) configValue;
            return "'" + stringValue.stringValue() + "'";
        }
        throw new IllegalArgumentException( "Could not convert config value '" + configValue + "' to config string." );
    }

    private static String propertiesAsStringArray( TokenRead tokenRead, SchemaDescriptor schema ) throws PropertyKeyIdNotFoundKernelException
    {
        StringJoiner properties = arrayStringJoiner();
        for ( int propertyId : schema.getPropertyIds() )
        {
            properties.add( "'" + tokenRead.propertyKeyName( propertyId ) + "'" );
        }
        return properties.toString();
    }

    private static String labelsOrRelTypesAsStringArray( TokenRead tokenRead, SchemaDescriptor schema ) throws KernelException
    {
        StringJoiner labelsOrRelTypes = arrayStringJoiner();
        for ( int entityTokenId : schema.getEntityTokenIds() )
        {
            if ( EntityType.NODE.equals( schema.entityType() ) )
            {
                labelsOrRelTypes.add( "'" + tokenRead.nodeLabelName( entityTokenId ) + "'" );
            }
            else
            {
                labelsOrRelTypes.add( "'" + tokenRead.relationshipTypeName( entityTokenId ) + "'" );
            }
        }
        return labelsOrRelTypes.toString();
    }

    private static String dropStatement( IndexDescriptor indexDescriptor )
    {
        return format( DROP_INDEX, indexDescriptor.getName() );
    }

    private static StringJoiner configStringJoiner()
    {
        return new StringJoiner( ",", "{", "}" );
    }

    private static StringJoiner arrayStringJoiner()
    {
        return new StringJoiner( ", ", "[", "]" );
    }

    enum SchemaRuleType
    {
        INDEX,
        CONSTRAINT
    }
}
