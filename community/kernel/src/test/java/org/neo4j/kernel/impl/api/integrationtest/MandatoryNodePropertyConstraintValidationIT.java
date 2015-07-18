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

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;

public class MandatoryNodePropertyConstraintValidationIT extends MandatoryPropertyConstraintValidationIT
{
    @Test
    public void shouldAllowNoopLabelUpdate() throws Exception
    {
        // given
        long entityId = createConstraintAndEntity( "Label1", "key1", "value1" );

        DataWriteOperations statement = dataWriteOperationsInNewTransaction();

        // when
        statement.nodeAddLabel( entityId, statement.labelGetOrCreateForName( "Label1" ) );

        // then should not throw exception
    }

    @Override
    void createConstraint( String key, String property ) throws KernelException
    {
        DataWriteOperations dataWrite = dataWriteOperationsInNewTransaction();
        int label = dataWrite.labelGetOrCreateForName( key );
        int propertyKey = dataWrite.propertyKeyGetOrCreateForName( property );
        commit();

        SchemaWriteOperations schemaWrite = schemaWriteOperationsInNewTransaction();
        schemaWrite.mandatoryNodePropertyConstraintCreate( label, propertyKey );
        commit();
    }

    @Override
    long createEntity( DataWriteOperations writeOps, String type ) throws Exception
    {
        long node = writeOps.nodeCreate();
        writeOps.nodeAddLabel( node, writeOps.labelGetOrCreateForName( type ) );
        return node;
    }

    @Override
    long createEntity( DataWriteOperations writeOps, String property, String value ) throws Exception
    {
        long node = writeOps.nodeCreate();
        int propertyKey = writeOps.propertyKeyGetOrCreateForName( property );
        writeOps.nodeSetProperty( node, Property.property( propertyKey, value ) );
        return node;
    }

    @Override
    long createEntity( DataWriteOperations writeOps, String type, String property, String value ) throws Exception
    {
        long node = createEntity( writeOps, type );
        int propertyKey = writeOps.propertyKeyGetOrCreateForName( property );
        writeOps.nodeSetProperty( node, Property.property( propertyKey, value ) );
        return node;
    }

    @Override
    long createConstraintAndEntity( String type, String property, String value ) throws Exception
    {
        DataWriteOperations dataWrite = dataWriteOperationsInNewTransaction();
        int label = dataWrite.labelGetOrCreateForName( type );
        long node = dataWrite.nodeCreate();
        dataWrite.nodeAddLabel( node, label );
        int propertyKey = dataWrite.propertyKeyGetOrCreateForName( property );
        dataWrite.nodeSetProperty( node, Property.property( propertyKey, value ) );
        commit();

        createConstraint( type, property );

        return node;
    }

    @Override
    void setProperty( DataWriteOperations writeOps, long entityId, DefinedProperty property ) throws Exception
    {
        writeOps.nodeSetProperty( entityId, property );
    }

    @Override
    void removeProperty( DataWriteOperations writeOps, long entityId, int propertyKey ) throws Exception
    {
        writeOps.nodeRemoveProperty( entityId, propertyKey );
    }

    @Override
    int entityCount() throws TransactionFailureException
    {
        ReadOperations readOps = readOperationsInNewTransaction();
        int result = PrimitiveLongCollections.count( readOps.nodesGetAll() );
        rollback();
        return result;
    }
}
