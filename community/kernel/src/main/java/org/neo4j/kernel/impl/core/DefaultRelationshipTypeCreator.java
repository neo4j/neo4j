/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.function.Supplier;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;

public class DefaultRelationshipTypeCreator extends IsolatedTransactionTokenCreator
{
    public DefaultRelationshipTypeCreator( Supplier<KernelAPI> kernelSupplier, IdGeneratorFactory idGeneratorFactory )
    {
        super( kernelSupplier, idGeneratorFactory );
    }

    @Override
    protected int createKey( Statement statement, String name ) throws IllegalTokenNameException
    {
        int id = (int) idGeneratorFactory.get( IdType.RELATIONSHIP_TYPE_TOKEN ).nextId();
        statement.tokenWriteOperations().relationshipTypeCreateForName( name,
                id );
        return id;
    }
}
