/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.kernel.impl.api.operations.CountsOperations;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.api.operations.ExplicitIndexReadOperations;
import org.neo4j.kernel.impl.api.operations.ExplicitIndexWriteOperations;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.api.operations.KeyWriteOperations;
import org.neo4j.kernel.impl.api.operations.LockOperations;
import org.neo4j.kernel.impl.api.operations.QueryRegistrationOperations;
import org.neo4j.kernel.impl.api.operations.SchemaReadOperations;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;
import org.neo4j.kernel.impl.api.operations.SchemaWriteOperations;

public class StatementOperationParts
{
    private final KeyReadOperations keyReadOperations;
    private final KeyWriteOperations keyWriteOperations;
    private final EntityReadOperations entityReadOperations;
    private final EntityWriteOperations entityWriteOperations;
    private final SchemaReadOperations schemaReadOperations;
    private final SchemaWriteOperations schemaWriteOperations;
    private final SchemaStateOperations schemaStateOperations;
    private final LockOperations lockingStatementOperations;
    private final CountsOperations countsStatementOperations;
    private final ExplicitIndexReadOperations explicitIndexReadOperations;
    private final ExplicitIndexWriteOperations explicitIndexWriteOperations;
    private final QueryRegistrationOperations queryRegistrationOperations;

    public StatementOperationParts(
            KeyReadOperations keyReadOperations,
            KeyWriteOperations keyWriteOperations,
            EntityReadOperations entityReadOperations,
            EntityWriteOperations entityWriteOperations,
            SchemaReadOperations schemaReadOperations,
            SchemaWriteOperations schemaWriteOperations,
            SchemaStateOperations schemaStateOperations,
            LockOperations lockingStatementOperations,
            CountsOperations countsStatementOperations,
            ExplicitIndexReadOperations explicitIndexReadOperations,
            ExplicitIndexWriteOperations explicitIndexWriteOperations,
            QueryRegistrationOperations queryRegistrationOperations )
    {
        this.keyReadOperations = keyReadOperations;
        this.keyWriteOperations = keyWriteOperations;
        this.entityReadOperations = entityReadOperations;
        this.entityWriteOperations = entityWriteOperations;
        this.schemaReadOperations = schemaReadOperations;
        this.schemaWriteOperations = schemaWriteOperations;
        this.schemaStateOperations = schemaStateOperations;
        this.lockingStatementOperations = lockingStatementOperations;
        this.countsStatementOperations = countsStatementOperations;
        this.explicitIndexReadOperations = explicitIndexReadOperations;
        this.explicitIndexWriteOperations = explicitIndexWriteOperations;
        this.queryRegistrationOperations = queryRegistrationOperations;
    }

    public KeyReadOperations keyReadOperations()
    {
        return checkNotNull( keyReadOperations, KeyReadOperations.class );
    }

    public KeyWriteOperations keyWriteOperations()
    {
        return checkNotNull( keyWriteOperations, KeyWriteOperations.class );
    }

    public EntityReadOperations entityReadOperations()
    {
        return checkNotNull( entityReadOperations, EntityReadOperations.class );
    }

    public EntityWriteOperations entityWriteOperations()
    {
        return checkNotNull( entityWriteOperations, EntityWriteOperations.class );
    }

    public SchemaReadOperations schemaReadOperations()
    {
        return checkNotNull( schemaReadOperations, SchemaReadOperations.class );
    }

    public SchemaWriteOperations schemaWriteOperations()
    {
        return checkNotNull( schemaWriteOperations, SchemaWriteOperations.class );
    }

    public SchemaStateOperations schemaStateOperations()
    {
        return checkNotNull( schemaStateOperations, SchemaStateOperations.class );
    }

    public LockOperations locking()
    {
        return checkNotNull( lockingStatementOperations, LockOperations.class );
    }

    public ExplicitIndexReadOperations explicitIndexReadOperations()
    {
        return checkNotNull( explicitIndexReadOperations, ExplicitIndexReadOperations.class );
    }

    public ExplicitIndexWriteOperations explicitIndexWriteOperations()
    {
        return checkNotNull( explicitIndexWriteOperations, ExplicitIndexWriteOperations.class );
    }

    public CountsOperations counting()
    {
        return checkNotNull( countsStatementOperations, CountsOperations.class );
    }

    public QueryRegistrationOperations queryRegistrationOperations()
    {
        return checkNotNull( queryRegistrationOperations, QueryRegistrationOperations.class );
    }

    public StatementOperationParts override(
            KeyReadOperations keyReadOperations,
            KeyWriteOperations keyWriteOperations,
            EntityReadOperations entityReadOperations,
            EntityWriteOperations entityWriteOperations,
            SchemaReadOperations schemaReadOperations,
            SchemaWriteOperations schemaWriteOperations,
            SchemaStateOperations schemaStateOperations,
            LockOperations lockingStatementOperations,
            CountsOperations countsStatementOperations,
            ExplicitIndexReadOperations explicitIndexReadOperations,
            ExplicitIndexWriteOperations explicitIndexWriteOperations,
            QueryRegistrationOperations queryRegistrationOperations )
    {
        return new StatementOperationParts(
            eitherOr( keyReadOperations, this.keyReadOperations, KeyReadOperations.class ),
            eitherOr( keyWriteOperations, this.keyWriteOperations, KeyWriteOperations.class ),
            eitherOr( entityReadOperations, this.entityReadOperations, EntityReadOperations.class ),
            eitherOr( entityWriteOperations, this.entityWriteOperations, EntityWriteOperations.class ),
            eitherOr( schemaReadOperations, this.schemaReadOperations, SchemaReadOperations.class ),
            eitherOr( schemaWriteOperations, this.schemaWriteOperations, SchemaWriteOperations.class ),
            eitherOr( schemaStateOperations, this.schemaStateOperations, SchemaStateOperations.class ),
            eitherOr( lockingStatementOperations, this.lockingStatementOperations, LockOperations.class ),
            eitherOr( countsStatementOperations, this.countsStatementOperations, CountsOperations.class ),
            eitherOr( explicitIndexReadOperations, this.explicitIndexReadOperations, ExplicitIndexReadOperations.class ),
            eitherOr( explicitIndexWriteOperations, this.explicitIndexWriteOperations, ExplicitIndexWriteOperations.class ),
            eitherOr( queryRegistrationOperations, this.queryRegistrationOperations, QueryRegistrationOperations.class ) );
    }

    private <T> T checkNotNull( T object, Class<T> cls )
    {
        if ( object == null )
        {
            throw new IllegalStateException( "No part of type " + cls.getSimpleName() + " assigned" );
        }
        return object;
    }

    private <T> T eitherOr( T first, T other,
            @SuppressWarnings( "UnusedParameters"/*used as type flag*/ ) Class<T> cls )
    {
        return first != null ? first : other;
    }
}
