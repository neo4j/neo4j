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
package org.neo4j.kernel.impl.device;

import org.neo4j.annotations.service.Service;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.service.PrioritizedService;
import org.neo4j.service.Services;

@Service
public interface DeviceMapperService extends PrioritizedService {

    DeviceMapper createDeviceMapper(InternalLogProvider logProvider);

    static DeviceMapperService getInstance() {
        return DeviceMapperProviderHolder.DEVICE_MAPPER_SERVICE_PROVIDER;
    }

    final class DeviceMapperProviderHolder {
        private static final DeviceMapperService DEVICE_MAPPER_SERVICE_PROVIDER = loadDeviceMapper();

        private DeviceMapperProviderHolder() {}

        private static DeviceMapperService loadDeviceMapper() {
            return Services.loadByPriority(DeviceMapperService.class)
                    .orElseThrow(
                            () -> new IllegalStateException("Failed to load instance of " + DeviceMapperService.class));
        }
    }
}
