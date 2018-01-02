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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;

public class SchemaRuleException extends SchemaKernelException
{

    protected final int ruleEntityId;
    protected final int propertyKeyId;
    protected final String messageTemplate;
    protected final String messagePrefix;

    protected SchemaRuleException( Status status, String messageTemplate, int ruleEntityId, int propertyKeyId,
            String messagePrefix)
    {
        super( status, format( messageTemplate, messagePrefix, ruleEntityId, propertyKeyId ) );
        this.ruleEntityId = ruleEntityId;
        this.propertyKeyId = propertyKeyId;
        this.messageTemplate = messageTemplate;
        this.messagePrefix = messagePrefix;
    }
}
