/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.consistency.checking;

import java.util.Objects;

import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;

public abstract class SchemaRuleKey
{
    private final boolean isUnique;
    private final SchemaDescriptor schema;

    protected SchemaRuleKey( SchemaDescriptor schema, boolean isUnique )
    {
        this.isUnique = isUnique;
        this.schema = schema;
    }

    public static SchemaRuleKey from( SchemaRule rule )
    {
        return rule instanceof ConstraintDescriptor ? new ConstraintKey( (ConstraintDescriptor) rule ) : new IndexKey( (IndexDescriptor) rule );
    }

    static class IndexKey extends SchemaRuleKey
    {
        private final IndexType type;

        IndexKey( IndexDescriptor index )
        {
            super( index.schema(), index.isUnique() );
            this.type = index.getIndexType();
        }

        @Override
        public boolean equals( Object others )
        {
            return super.equals( others ) && type == ((IndexKey) others).type;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( type, super.hashCode() );
        }
    }

    static class ConstraintKey extends SchemaRuleKey
    {
        ConstraintKey( ConstraintDescriptor constraint )
        {
            super( constraint.schema(), constraint.enforcesUniqueness() );
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        SchemaRuleKey that = (SchemaRuleKey) o;
        return isUnique == that.isUnique && schema.equals( that.schema );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( isUnique ? 1 : 0, schema.hashCode() );
    }
}
