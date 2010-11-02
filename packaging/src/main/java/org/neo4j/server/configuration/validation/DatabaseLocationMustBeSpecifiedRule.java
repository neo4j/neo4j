/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.configuration.validation;

import org.apache.commons.configuration.Configuration;

public class DatabaseLocationMustBeSpecifiedRule implements ValidationRule {
    private static final String ORG_NEO4J_DATABASE_LOCATION = "org.neo4j.database.location";

    public void validate(Configuration configuration) throws RuleFailedException {
        String dbLocation = configuration.getString(ORG_NEO4J_DATABASE_LOCATION);
        if(dbLocation == null || dbLocation.length() < 1) {
            throw new RuleFailedException("The key [%s] is missing from the Neo Server configuration.", ORG_NEO4J_DATABASE_LOCATION);
        }
    }

}
