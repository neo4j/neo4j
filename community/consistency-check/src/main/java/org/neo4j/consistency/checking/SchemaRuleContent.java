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
package org.neo4j.consistency.checking;

import org.neo4j.kernel.impl.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;

public class SchemaRuleContent
{
    private final SchemaRule schemaRule;

    public SchemaRuleContent( SchemaRule schemaRule )
    {
        this.schemaRule = schemaRule;
    }

    @Override
    public String toString()
    {
        return "ContentOf:" + schemaRule.toString();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj instanceof SchemaRuleContent )
        {
            SchemaRuleContent that = (SchemaRuleContent) obj;
            if ( this.schemaRule.getLabel() != that.schemaRule.getLabel() )
            {
                return false;
            }
            switch ( schemaRule.getKind() )
            {
                case INDEX_RULE:
                case CONSTRAINT_INDEX_RULE:
                    if ( !that.schemaRule.getKind().isIndex() )
                    {
                        return false;
                    }
                    return indexRulesEquals( (IndexRule) this.schemaRule, (IndexRule) that.schemaRule );
                case UNIQUENESS_CONSTRAINT:
                    return this.schemaRule.getKind() == that.schemaRule.getKind() && uniquenessConstraintEquals(
                            (UniquenessConstraintRule) this.schemaRule,
                            (UniquenessConstraintRule) that.schemaRule );
                default:
                    throw new IllegalArgumentException( "Invalid SchemaRule kind: " + schemaRule.getKind() );
            }
        }
        return false;
    }

    private static boolean indexRulesEquals( IndexRule lhs, IndexRule rhs )
    {
        return lhs.getPropertyKey() == rhs.getPropertyKey();
    }

    private static boolean uniquenessConstraintEquals( UniquenessConstraintRule lhs, UniquenessConstraintRule rhs )
    {
        return lhs.getPropertyKey() == rhs.getPropertyKey();
    }

    @Override
    public int hashCode()
    {
        return (int) schemaRule.getLabel();
    }
}
