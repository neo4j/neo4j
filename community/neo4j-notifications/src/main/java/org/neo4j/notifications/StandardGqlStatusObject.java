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
package org.neo4j.notifications;

import org.neo4j.gqlstatus.CommonGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.DiagnosticRecord;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.GqlStatusObject;

public class StandardGqlStatusObject extends CommonGqlStatusObjectImplementation implements GqlStatusObject {

    public static final StandardGqlStatusObject SUCCESS = new StandardGqlStatusObject(GqlStatusInfoCodes.STATUS_00000);
    public static final StandardGqlStatusObject NO_DATA = new StandardGqlStatusObject(GqlStatusInfoCodes.STATUS_02000);
    public static final StandardGqlStatusObject UNKNOWN_NO_DATA =
            new StandardGqlStatusObject(GqlStatusInfoCodes.STATUS_02N42);
    public static final StandardGqlStatusObject OMITTED_RESULT =
            new StandardGqlStatusObject(GqlStatusInfoCodes.STATUS_00001);

    StandardGqlStatusObject(GqlStatusInfoCodes gqlStatusInfo) {
        super(gqlStatusInfo, new DiagnosticRecord());
    }

    @Override
    public String toString() {
        if (this == SUCCESS) {
            return "SUCCESS";
        }
        if (this == NO_DATA) {
            return "NO DATA";
        }
        if (this == UNKNOWN_NO_DATA) {
            return "UNKNOWN NO DATA";
        }
        if (this == OMITTED_RESULT) {
            return "OMITTED RESULT";
        }
        return String.format("GqlStatusObject with GQLSTATUS %s", gqlStatusInfo.getStatusString());
    }

    public static boolean isStandardGqlStatusCode(GqlStatusObject gso) {
        return gso == StandardGqlStatusObject.NO_DATA
                || gso == StandardGqlStatusObject.SUCCESS
                || gso == StandardGqlStatusObject.OMITTED_RESULT
                || gso == StandardGqlStatusObject.UNKNOWN_NO_DATA;
    }
}
