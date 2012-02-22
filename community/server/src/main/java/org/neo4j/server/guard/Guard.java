/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.guard;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Guard {

    private static int totalOpsCount = 0;
    private static final Log LOG = LogFactory.getLog(GuardingRequestFilter.class);

    private final ThreadLocal<GuardInternal> threadLocal = new ThreadLocal<GuardInternal>();
    private final Timer timer = new Timer();

    private int timeLimit;

    public Guard(final int timeLimit) {
        this.timeLimit = timeLimit;
    }

    public int getTimeLimit() {
        return timeLimit;
    }

    public void check() {
        GuardInternal guardInternal = threadLocal.get();
        if (guardInternal != null) {
            guardInternal.check();
        }
    }

    public void start(final long valid) {
        threadLocal.set(new GuardInternal(valid));
    }

    public void stop() {
        GuardInternal guardInternal = threadLocal.get();
        if (guardInternal != null) {
            guardInternal.stop();
            clear();
        }
    }

    public void clear() {
        threadLocal.remove();
    }

    private class GuardInternal {
        private final long valid;
        private final Thread current = currentThread();
        private int opsCount = 0;

        private final TimerTask timerTask = new TimerTask() {
            @Override
            public void run() {
                LOG.warn("request canceld");
                LOG.error("TODO: restarting the server is not proper implemented, request was not canceled");
                // TODO current.interrupt(); + restart server
            }
        };

        private GuardInternal(final long valid) {
            timer.schedule(timerTask, valid + 2000);

            this.valid = valid;
        }

        private void check() {
            totalOpsCount++;
            opsCount++;

            if (valid < currentTimeMillis()) {
                final long overtime = currentTimeMillis() - valid;
                LOG.error("timeout! " + overtime);
                throw new GuardException(opsCount, totalOpsCount, overtime);
            }
        }

        public void stop() {
            timerTask.cancel();
        }

    }

}
