/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.packstream.error.struct;

import java.util.List;
import org.neo4j.bolt.fsm.error.ConnectionTerminating;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.GqlHelper;

public class FatalIllegalStructArgumentException extends IllegalStructArgumentException
        implements ConnectionTerminating {
    public FatalIllegalStructArgumentException(
            ErrorGqlStatusObject gqlStatusObject, String fieldName, String message, Throwable cause) {
        super(gqlStatusObject, fieldName, message, cause);
    }

    public static FatalIllegalStructArgumentException wrongTypeForFieldName(
            String fieldName, String value, List<String> expectedType, String actualType, String message) {
        return new FatalIllegalStructArgumentException(
                GqlHelper.getGql08N06(GqlHelper.getGql22G03_22N01(value, expectedType, actualType)),
                fieldName,
                message,
                null);
    }
}
