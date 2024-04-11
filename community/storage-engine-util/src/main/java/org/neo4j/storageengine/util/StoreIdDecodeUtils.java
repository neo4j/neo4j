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
package org.neo4j.storageengine.util;

import static org.neo4j.internal.helpers.Format.hexString;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.StoreIdProvider;

public class StoreIdDecodeUtils {
    private static final String DEFAULT_ALGORITHM = "SHA-256";

    private StoreIdDecodeUtils() {}

    public static String decodeId(StoreIdProvider storeIdProvider) {
        return decodeId(storeIdProvider.getExternalStoreId());
    }

    public static String decodeId(ExternalStoreId externalStoreId) {
        try {
            var storeIdString = externalStoreId.id().toString();
            var messageDigest = MessageDigest.getInstance(DEFAULT_ALGORITHM);
            messageDigest.update(storeIdString.getBytes());
            return hexString(messageDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
