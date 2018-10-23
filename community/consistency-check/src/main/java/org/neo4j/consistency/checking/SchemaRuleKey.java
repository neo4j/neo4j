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
package org.neo4j.consistency.checking;

import java.util.Objects;

import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;

public class SchemaRuleKey
{
    private final boolean isConstraint;
    private final boolean isUnique;
    private final SchemaDescriptor schema;

    public SchemaRuleKey( SchemaRule rule )
    {
        if ( rule instanceof ConstraintDescriptor )
        {
            ConstraintDescriptor constraint = (ConstraintDescriptor) rule;
            this.isConstraint = true;
            this.isUnique = constraint.enforcesUniqueness();
        }
        else
        {
            IndexDescriptor index = (IndexDescriptor) rule;
            this.isConstraint = false;
            this.isUnique = index.isUnique();
        }
        this.schema = rule.schema();
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

        if ( isConstraint != that.isConstraint )
        {
            return false;
        }
        if ( isUnique != that.isUnique )
        {
            return false;
        }
        return schema.equals( that.schema );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( isConstraint ? 1 : 0, isUnique ? 1 : 0, schema.hashCode() );
    }
}
