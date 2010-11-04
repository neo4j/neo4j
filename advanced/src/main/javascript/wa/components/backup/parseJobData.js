
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