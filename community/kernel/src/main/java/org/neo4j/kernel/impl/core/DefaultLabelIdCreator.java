/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.helpers.Provider;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;

public class DefaultLabelIdCreator extends IsolatedTransactionTokenCreator
{
    public DefaultLabelIdCreator( Provider<KernelAPI> kernelProvider, IdGeneratorFactory idGeneratorFactory )
    {
        super( kernelProvider, idGeneratorFactory );
    }

    @Override
    protected int createKey( TransactionRecordState transactionRecordState, String name )
    {
        int id = (int) idGeneratorFactory.get( IdType.LABEL_TOKEN ).nextId();
        transactionRecordState.createLabelToken( name, id );
        return id;
    }
}
