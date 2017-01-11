require('colors');
var exec    = require('child_process').exec;

module.exports = function (command, args, options, callback) {
  var times = 1;
  function run () {
    var runCmd = command + ' ' + args;
    console.log(('attempt ' + times).bold.green + ': ' + runCmd);

    process.env.npm_config_color = 0;

    var attempt = exec(runCmd, function (err, stdout, stderr) {
      if (stdout.match(/npm ERR\! cb\(\) never called\!/ig) || stderr.match(/npm ERR\! cb\(\) never called\!/ig)) {
        if (times >= options.attempts) {
          return callback(new Error('too many attempts'));
        }
        times++;
        return setTimeout(run, options.wait);
      }
      return callback(null, {times: times, stdout: stdout, exitCode: (err && err.code) || 0});
    });
    
    attempt.stdout.pipe(process.stdout);
    attempt.stderr.pipe(process.stderr);
  }
  run();
};
