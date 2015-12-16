/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.storageengine.api.schema;

import org.neo4j.kernel.api.exceptions.schema.MalformedSchemaRuleException;

/**
 * Represents a stored schema rule.
 */
public interface SchemaRule
{
    /**
     * The persistence id for this rule.
     */
    long getId();

    /**
     * @return id of label to which this schema rule has been attached
     * @throws IllegalStateException when this rule has kind {@link Kind#RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT}
     */
    int getLabel();

    /**
     * @return id of relationship type to which this schema rule has been attached
     * @throws IllegalStateException when this rule has kind different from
     * {@link Kind#RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT}
     */
    int getRelationshipType();

    /**
     * @return the kind of this schema rule
     */
    Kind getKind();

    enum Kind
    {
        INDEX_RULE( true, false ),
        CONSTRAINT_INDEX_RULE( true, true ),
        UNIQUENESS_CONSTRAINT( false, true ),
        NODE_PROPERTY_EXISTENCE_CONSTRAINT( false, true ),
        RELATIONSHIP_PROPERTY_EXISTENCE_CONSTRAINT( false, true );

        private static final Kind[] ALL = values();

        private final boolean isIndex;
        private final boolean isConstraint;

        private Kind( boolean isIndex, boolean isConstraint )
        {
            this.isIndex = isIndex;
            this.isConstraint = isConstraint;
        }

        public byte id()
        {
            return (byte) (ordinal() + 1);
        }

        public boolean isConstraint()
        {
            return isConstraint;
        }

        public boolean isIndex()
        {
            return isIndex;
        }

        public static Kind forId( byte id ) throws MalformedSchemaRuleException
        {
            if ( id >= 1 && id <= ALL.length )
            {
                return values()[id-1];
            }
            throw new MalformedSchemaRuleException( null, "Unknown kind id %d", id );
        }
    }
}
