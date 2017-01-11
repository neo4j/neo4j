var npm_install_retry = require('../');
var expect            = require('chai').expect;

function Fail3TimesCommand() { this.times = 0; }

Fail3TimesCommand.prototype.toString = function() {
  if(this.times < 3) {
    this.times++;
    return 'echo npm ERR\\! cb\\(\\) never called\\! 1>&2';
  }
  return 'echo peace and love';
};

describe('npm-install-retry', function () {
  
  it('should retry after failed', function (done) {
    npm_install_retry(new Fail3TimesCommand(), '', { wait: 0, attempts: 10 }, function (err, result) {
      if (err) return done(err);
      expect(result.times).to.eql(4);
      done();
    });
  });

  it('should fail if it fail all attempts', function (done) {
    npm_install_retry('echo npm ERR\\! cb\\(\\) never called\\! 1>&2', '', { wait: 0, attempts: 10 }, function (err, result) {
      expect(err.message).to.eql('too many attempts');
      done();
    });
  });

  it('should have npm_config_color false', function (done) {
    npm_install_retry('echo $npm_config_color', '', { wait: 0, attempts: 10 }, function (err, result) {
      if (err) return done(err);
      expect(result.stdout.split('\n')[0]).to.eql('0');
      done();
    });
  });

});