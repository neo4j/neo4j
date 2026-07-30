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
package org.neo4j.memory;

import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.General.MemoryPoolOutOfMemoryError;
import static org.neo4j.kernel.api.exceptions.Status.General.TransactionOutOfMemoryError;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.gqlstatus.NonSensitiveException;
import org.neo4j.kernel.api.exceptions.Status;

public class MemoryLimitExceededException extends GqlRuntimeException
        implements Status.HasStatus, NonSensitiveException {
    private final Status status;
    private final String settingName;

    private MemoryLimitExceededException(
            ErrorGqlStatusObject gqlStatusObject, Status status, String settingName, String message) {
        super(gqlStatusObject, message);
        this.status = status;
        this.settingName = settingName;
    }

    public static MemoryLimitExceededException memoryPoolOutOfMemoryExceeded(
            long allocation, long limit, long current, String settingName) {
        return memoryPoolOutOfMemoryExceeded(getMessage(allocation, limit, current, settingName), settingName);
    }

    public static MemoryLimitExceededException memoryPoolOutOfMemoryExceeded(String message, String settingName) {
        // KNL-008 and KNL-009
        var gqlStatusObject = getPoolOutOfMemoryGqlStatus(settingName);
        return new MemoryLimitExceededException(gqlStatusObject, MemoryPoolOutOfMemoryError, settingName, message);
    }

    public static MemoryLimitExceededException transactionMemoryLimitExceeded(
            long allocation, long limit, long current, String settingName) {
        return transactionMemoryLimitExceeded(getMessage(allocation, limit, current, settingName), settingName);
    }

    public static MemoryLimitExceededException transactionMemoryLimitExceeded(String message, String settingName) {
        // KNL-010
        var gqlStatusObject = getTransactionMemoryLimitExceededGqlStatus(settingName);
        return new MemoryLimitExceededException(gqlStatusObject, TransactionOutOfMemoryError, settingName, message);
    }

    public static ErrorGqlStatusObject getTransactionMemoryLimitExceededGqlStatus(String settingName) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N73)
                .withParam(GqlParams.StringParam.cfgSetting, settingName)
                .build();
    }

    public static ErrorGqlStatusObject getPoolOutOfMemoryGqlStatus(String settingName) {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N72)
                .withParam(GqlParams.StringParam.cfgSetting, settingName)
                .build();
    }

    @Override
    public Status status() {
        return status;
    }

    private static String getMessage(long allocation, long limit, long current, String settingName) {
        if (StringUtils.isEmpty(settingName)) {
            return format(
                    "The allocation of an extra %s would use more than the limit %s. Currently using %s.",
                    humanReadableByteCountBin(allocation),
                    humanReadableByteCountBin(limit),
                    humanReadableByteCountBin(current));
        }

        return format(
                "The allocation of an extra %s would use more than the limit %s. Currently using %s. %s threshold reached",
                humanReadableByteCountBin(allocation),
                humanReadableByteCountBin(limit),
                humanReadableByteCountBin(current),
                settingName);
    }

    private static String humanReadableByteCountBin(long bytes) {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(bytes);
        if (absB < 1024) {
            return bytes + " B";
        }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator("KMGTPE");
        for (int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10) {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum(bytes);
        return String.format("%.1f %ciB", value / 1024.0, ci.current());
    }

    public String getSettingName() {
        return settingName;
    }
}
