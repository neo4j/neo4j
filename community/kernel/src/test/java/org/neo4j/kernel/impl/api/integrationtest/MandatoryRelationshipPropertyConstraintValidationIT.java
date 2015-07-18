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
package org.neo4j.kernel.impl.api.integrationtest;

import java.util.UUID;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;

public class MandatoryRelationshipPropertyConstraintValidationIT extends MandatoryPropertyConstraintValidationIT
{
    @Override
    void createConstraint( String key, String property ) throws KernelException
    {
        DataWriteOperations dataWrite = dataWriteOperationsInNewTransaction();
        int relTypeId = dataWrite.relationshipTypeGetOrCreateForName( key );
        int propertyKeyId = dataWrite.propertyKeyGetOrCreateForName( property );
        commit();

        SchemaWriteOperations schemaWrite = schemaWriteOperationsInNewTransaction();
        schemaWrite.mandatoryRelationshipPropertyConstraintCreate( relTypeId, propertyKeyId );
        commit();
    }

    @Override
    long createEntity( DataWriteOperations writeOps, String type ) throws Exception
    {
        long start = writeOps.nodeCreate();
        long end = writeOps.nodeCreate();
        int relType = writeOps.relationshipTypeGetOrCreateForName( type );
        return writeOps.relationshipCreate( relType, start, end );
    }

    @Override
    long createEntity( DataWriteOperations writeOps, String property, String value ) throws Exception
    {
        long start = writeOps.nodeCreate();
        long end = writeOps.nodeCreate();
        int relType = writeOps.relationshipTypeGetOrCreateForName( UUID.randomUUID().toString() );
        long relationship = writeOps.relationshipCreate( relType, start, end );

        int propertyKey = writeOps.propertyKeyGetOrCreateForName( property );
        writeOps.relationshipSetProperty( relationship, Property.property( propertyKey, value ) );
        return relationship;
    }

    @Override
    long createEntity( DataWriteOperations writeOps, String type, String property, String value ) throws Exception
    {
        long relationship = createEntity( writeOps, type );
        int propertyKey = writeOps.propertyKeyGetOrCreateForName( property );
        writeOps.relationshipSetProperty( relationship, Property.property( propertyKey, value ) );
        return relationship;
    }

    @Override
    long createConstraintAndEntity( String type, String property, String value ) throws Exception
    {
        DataWriteOperations dataWrite = dataWriteOperationsInNewTransaction();
        int relType = dataWrite.relationshipTypeGetOrCreateForName( type );
        long start = dataWrite.nodeCreate();
        long end = dataWrite.nodeCreate();
        long relationship = dataWrite.relationshipCreate( relType, start, end );
        int propertyKey = dataWrite.propertyKeyGetOrCreateForName( property );
        dataWrite.relationshipSetProperty( relationship, Property.property( propertyKey, value ) );
        commit();

        createConstraint( type, property );

        return relationship;
    }

    @Override
    void setProperty( DataWriteOperations writeOps, long entityId, DefinedProperty property ) throws Exception
    {
        writeOps.relationshipSetProperty( entityId, property );
    }

    @Override
    void removeProperty( DataWriteOperations writeOps, long entityId, int propertyKey ) throws Exception
    {
        writeOps.relationshipRemoveProperty( entityId, propertyKey );
    }

    @Override
    int entityCount() throws TransactionFailureException
    {
        ReadOperations readOps = readOperationsInNewTransaction();
        int result = PrimitiveLongCollections.count( readOps.relationshipsGetAll() );
        rollback();
        return result;
    }
}
