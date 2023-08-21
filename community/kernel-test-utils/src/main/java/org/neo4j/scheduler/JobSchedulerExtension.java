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
package org.neo4j.scheduler;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.test.extension.StatefulFieldExtension;

public class JobSchedulerExtension extends StatefulFieldExtension<JobScheduler> implements AfterEachCallback {
    private static final String FIELD_KEY = "job_scheduler";
    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(FIELD_KEY);

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        JobScheduler jobScheduler = deepRemoveStoredValue(context);
        jobScheduler.close();
    }

    @Override
    protected String getFieldKey() {
        return FIELD_KEY;
    }

    @Override
    protected ExtensionContext.Namespace getNameSpace() {
        return NAMESPACE;
    }

    @Override
    protected Class<JobScheduler> getFieldType() {
        return JobScheduler.class;
    }

    @Override
    protected JobScheduler createField(ExtensionContext context) {
        return JobSchedulerFactory.createInitialisedScheduler();
    }
}
