/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.KernelTransaction;

/**
 * Adds constraint checking to the kernel implementation, for instance ensuring label names are valid.
 */
public class ConstraintValidatingTransactionContext extends DelegatingTransactionContext
{
    // Note: This could be refactored to use arbitrary constraint rules, so this could evaluate
    // both user and system level constraints.

    public ConstraintValidatingTransactionContext( KernelTransaction delegate )
    {
        super( delegate );
    }

    @Override
    public StatementOperationParts newStatementOperations()
    {
        StatementOperationParts parts = delegate.newStatementOperations();
        
        // + Constraints
        DataIntegrityValidatingStatementOperations dataIntegrityContext = new DataIntegrityValidatingStatementOperations(
                parts.keyWriteOperations(),
                parts.schemaReadOperations(),
                parts.schemaWriteOperations() );

        parts.replace( null, dataIntegrityContext, null, null, null, dataIntegrityContext, null, null );
        return parts;
    }
}
