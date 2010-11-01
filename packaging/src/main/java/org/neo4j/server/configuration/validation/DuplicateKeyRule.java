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

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.neo4j.server.logging.Logger;

public class DuplicateKeyRule implements ValidationRule {

    public static Logger log = Logger.getLogger(DuplicateKeyRule.class);

    @SuppressWarnings("unchecked")
    public void validate(Configuration existingConfiguration, Configuration additionalConfiguration) {
        Iterator<String> existingKeys = existingConfiguration.getKeys();
        while (existingKeys.hasNext()) {
            Iterator<String> additionalKeys = additionalConfiguration.getKeys();
            while (additionalKeys.hasNext()) {
                String existingKey = existingKeys.next();
                String additionalKey = additionalKeys.next();
                if (existingKey.equals(additionalKey)) {
                    log.info("Duplicate key [%s] found in configuration files", existingKey);
                    if (!existingConfiguration.getString(existingKey).equals(additionalConfiguration.getString(additionalKey))) {
                        throw new RuleFailedException(String.format("Key [%s] has existing value [%s], cannot overwrite it with new value [%s]", existingKey,
                                existingConfiguration.getString(existingKey), additionalConfiguration.getString(additionalKey)));

                    }
                }
            }
        }
    }
}
