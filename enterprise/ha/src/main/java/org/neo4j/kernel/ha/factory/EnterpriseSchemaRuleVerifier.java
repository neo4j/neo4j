/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.factory;

import java.util.Iterator;

import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.kernel.SchemaRuleVerifier;
import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.store.record.SchemaRule;

class EnterpriseSchemaRuleVerifier implements SchemaRuleVerifier
{
    @Override
    public void verify( SchemaRule rule )
    {
    }

    @Override
    public void assertPropertyConstraintCreationAllowed()
    {
    }

    @Override
    public TxStateVisitor createVerifierFor( StatementOperationParts operations, StoreStatement storeStatement,
            StoreReadLayer storeLayer, TxStateHolder holder, TxStateVisitor visitor ) {
        Iterator<PropertyConstraint> constraints = storeLayer.constraintsGetAll();
        while ( constraints.hasNext() )
        {
            PropertyConstraint constraint = constraints.next();
            if ( constraint.type() == ConstraintType.NODE_PROPERTY_EXISTENCE ||
                 constraint.type() == ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE )
            {
                return new PropertyExistenceEnforcer( operations.entityReadOperations(), visitor,
                        holder, storeLayer, storeStatement );
            }
        }
        return visitor;
    }
}
