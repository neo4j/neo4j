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
import java.util.StringJoiner;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;

final class SchemaStatementProcedure
{
    private SchemaStatementProcedure()
    {
    }

    static Collection<BuiltInProcedures.SchemaStatementResult> createSchemaStatementResults( SchemaReadCore schemaRead, TokenRead tokenRead )
            throws ProcedureException
    {
        Map<String,BuiltInProcedures.SchemaStatementResult> schemaStatements = new HashMap<>();

        // Indexes
        // If index is backing an existing constraint, it will be overwritten later.
        final Iterator<IndexDescriptor> allIndexes = schemaRead.indexesGetAll();
        while ( allIndexes.hasNext() )
        {
            final IndexDescriptor index = allIndexes.next();
            final String name = index.getName();
            final String createStatement = createStatement( tokenRead, index );
            final String dropStatement = dropStatement( index );
            schemaStatements.put( name, new BuiltInProcedures.SchemaStatementResult( name, "INDEX", createStatement, dropStatement ) );
        }

        // Constraints
        Iterator<ConstraintDescriptor> allConstraints = schemaRead.constraintsGetAll();
        while ( allConstraints.hasNext() )
        {
            ConstraintDescriptor constraint = allConstraints.next();
            String name = constraint.getName();
            String createStatement = createStatement( schemaRead, tokenRead, constraint );
            String dropStatement = dropStatement( constraint );
            schemaStatements.put( name, new BuiltInProcedures.SchemaStatementResult( name, "CONSTRAINT", createStatement, dropStatement ) );
        }

        return schemaStatements.values();
    }

    private static String createStatement( SchemaReadCore schemaRead, TokenRead tokenRead, ConstraintDescriptor constraint ) throws ProcedureException
    {
        try
        {
            String name = constraint.getName();
            if ( constraint.isIndexBackedConstraint() )
            {
                final String labelsOrRelTypes = labelsOrRelTypesAsStringArray( tokenRead, constraint.schema() );
                final String properties = propertiesAsStringArray( tokenRead, constraint.schema() );
                IndexDescriptor backingIndex = schemaRead.indexGetForName( name );
                String providerName = backingIndex.getIndexProvider().name();
                String config = btreeConfigAsString( backingIndex );
                if ( constraint.isUniquenessConstraint() )
                {
                    return format( "CALL db.createUniquePropertyConstraint( '%s', %s, %s, '%s', %s )",
                            name, labelsOrRelTypes, properties, providerName, config );
                }
                if ( constraint.isNodeKeyConstraint() )
                {
                    return format( "CALL db.createNodeKey( '%s', %s, %s, '%s', %s )",
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
                return format( "CREATE CONSTRAINT `%s` ON (a:`%s`) ASSERT exists(a.`%s`)",
                        name, label, property );
            }
            if ( constraint.isRelationshipPropertyExistenceConstraint() )
            {
                // "create CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)"
                int relationshipTypeId = constraint.schema().getRelTypeId();
                String relationshipType = tokenRead.relationshipTypeName( relationshipTypeId );
                int propertyId = constraint.schema().getPropertyId();
                String property = tokenRead.propertyKeyName( propertyId );
                return format( "CREATE CONSTRAINT `%s` ON ()-[a:`%s`]-() ASSERT exists(a.`%s`)",
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
        return "DROP CONSTRAINT `" + constraint.getName() + "`";
    }

    private static String createStatement( TokenRead tokenRead, IndexDescriptor indexDescriptor ) throws ProcedureException
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
                return format( "CALL db.createIndex('%s', %s, %s, '%s', %s)",
                        name, labelsOrRelTypes, properties, providerName, btreeConfig );
            case FULLTEXT:
                String fulltextConfig = fulltextConfigAsString( indexDescriptor );
                switch ( indexDescriptor.schema().entityType() )
                {
                case NODE:
                    return format( "CALL db.index.fulltext.createNodeIndex('%s', %s, %s, %s)",
                            name, labelsOrRelTypes, properties, fulltextConfig );
                case RELATIONSHIP:
                    return format( "CALL db.index.fulltext.createRelationshipIndex('%s', %s, %s, %s)",
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
        StringJoiner configString = new StringJoiner( ",", "{", "}" );
        for ( Pair<String,Value> entry : indexConfig.entries() )
        {
            String singleConfig = "`" + entry.getOne() + "`: " + btreeConfigValueAsString( entry.getTwo() );
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
        StringJoiner configString = new StringJoiner( ",", "{", "}" );
        for ( Pair<String,Value> entry : indexConfig.entries() )
        {
            String key = entry.getOne();
            key = key.replace( "fulltext.", "" );
            String singleConfig = "`" + key + "`: " + fulltextConfigValueAsString( entry.getTwo() );
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
        StringJoiner properties = new StringJoiner( ", ", "[", "]" );
        for ( int propertyId : schema.getPropertyIds() )
        {
            properties.add( "'" + tokenRead.propertyKeyName( propertyId ) + "'" );
        }
        return properties.toString();
    }

    private static String labelsOrRelTypesAsStringArray( TokenRead tokenRead, SchemaDescriptor schema ) throws KernelException
    {
        StringJoiner labelsOrRelTypes = new StringJoiner( ", ", "[", "]" );
        for ( int entityTokenId : schema.getEntityTokenIds() )
        {
            if ( schema.entityType().equals( EntityType.NODE ) )
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
        return "DROP INDEX `" + indexDescriptor.getName() + "`";
    }
}
