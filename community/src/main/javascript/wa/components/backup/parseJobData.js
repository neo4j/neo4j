/*
 * Copyright (c) 2002-2011 "Neo Technology,"
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

/**
 * Goes through backup job data provided by the server. Adds human-readable
 * dates (e.g. "One day ago" etc), and makes any currently blocking errors
 * easily accessible.
 */
wa.components.backup.parseJobData = function(data) {
    if (data != null)
    {

        for ( var i = 0, l = data.jobList.length; i < l; i++)
        {

            // Write last-backup message (used by templates)
            var job = data.jobList[i];
            if (job.log.latestSuccess == null)
            {
                job.readableLatestSuccess = "Never";
            } else
            {
                var now = new Date().getTime();
                var diff = (now - job.log.latestSuccess.timestamp) / 1000;

                var readableDiff = "";
                if (diff > 60 * 60 * 24 * 2)
                {
                    // Over two days ago, only worry about days
                    readableDiff = Math.floor(diff / (60 * 60 * 24))
                            + " days ago";
                } else if (diff > 60 * 60 * 24)
                {
                    // Over one day
                    readableDiff = "One day and "
                            + Math.floor((diff - 60 * 60 * 24) / (60 * 60))
                            + " hours ago";
                } else if (diff > 60 * 60)
                {
                    // Over one hour
                    readableDiff = Math.floor(diff / (60 * 60)) + " hours ago";
                } else if (diff > 60)
                {
                    // Over one minute
                    readableDiff = Math.floor(diff / 60) + " minutes ago";
                } else
                {
                    // Less than a minute
                    readableDiff = "Less than a minute ago";
                }

                job.readableLatestSuccess = readableDiff;
            }

            // Provide easy access to any blocking error (used by templates)
            if (job.log.entries.length > 0
                    && job.log.entries[0].type === "ERROR")
            {
                job.error = {
                    message : job.log.entries[0].message,
                    timestamp : job.log.entries[0].timestamp,
                    code : job.log.entries[0].code
                };
            } else
            {
                job.error = false;
            }
        }
    }

    return data;
};