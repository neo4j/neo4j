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
package org.neo4j.kernel.impl.constraints;

import org.neo4j.kernel.api.constraints.PropertyConstraint;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.api.txstate.TxStateVisitor;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.api.store.StoreStatement;
import org.neo4j.kernel.impl.store.record.PropertyConstraintRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;

public class StandardConstraintSemantics implements ConstraintSemantics
{
    @Override
    public void assertPropertyConstraintCreationAllowed()
    {
        throw propertyExistenceConstraintsNotAllowed();
    }

    @Override
    public PropertyConstraint readConstraint( PropertyConstraintRule rule )
    {
        if ( rule instanceof UniquePropertyConstraintRule )
        {
            return ((UniquePropertyConstraintRule) rule).toConstraint();
        }
        return readNonStandardConstraint( rule );
    }

    protected PropertyConstraint readNonStandardConstraint( PropertyConstraintRule rule )
    {
        // todo: message and new exception type
        throw new IllegalStateException( "Property existence constraints can only be used on Neo4j enterprise" );
    }

    private IllegalStateException propertyExistenceConstraintsNotAllowed()
    {
        // todo: message and new exception type
        return new IllegalStateException( "Property existence constraints can only be used on Neo4j enterprise" );
    }

    @Override
    public PropertyConstraintRule writeUniquePropertyConstraint( long ruleId, int label, int propertyKey, long indexId )
    {
        return UniquePropertyConstraintRule.uniquenessConstraintRule( ruleId, label, propertyKey, indexId );
    }

    @Override
    public PropertyConstraintRule writeNodePropertyExistenceConstraint( long ruleId, int type, int propertyKey )
    {
        throw propertyExistenceConstraintsNotAllowed();
    }

    @Override
    public PropertyConstraintRule writeRelationshipPropertyExistenceConstraint( long ruleId, int type, int propertyKey )
    {
        throw propertyExistenceConstraintsNotAllowed();
    }

    @Override
    public TxStateVisitor decorateTxStateVisitor( StatementOperationParts operations, StoreStatement storeStatement,
            StoreReadLayer storeLayer, TxStateHolder holder, TxStateVisitor visitor )
    {
        return visitor;
    }
}
