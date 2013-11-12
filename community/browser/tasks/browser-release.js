/*
 * Based on original work by...
 *
 * grunt-release
 * https://github.com/geddski/grunt-release
 *
 * Copyright (c) 2013 Dave Geddes
 * Licensed under the MIT license.
 */

var shell = require('shelljs');
var semver = require('semver');

module.exports = function(grunt){
  grunt.registerTask('release', 'bump version, deploy with mvn', function(type){
    //defaults
    var options = this.options({
      bump: true,
      file: grunt.config('pkgFile') || 'package.json',
      add: true,
      commit: true,
      tag: true,
      push: true,
      pushTags: true,
      publish: false
    });

    var tagName = grunt.config.getRaw('release.options.tagName') || '<%= version %>';
    var commitMessage = grunt.config.getRaw('release.options.commitMessage') || 'release <%= version %>';
    var tagMessage = grunt.config.getRaw('release.options.tagMessage') || 'version <%= version %>';

    var config = setup(options.file, type);
    var templateOptions = {
      data: {
        version: config.releaseVersion
      }
    };

    bump(config, config.releaseVersion);
    add(config);
    commit(config);

    // prepareMavenRelease(config);
    // performMavenRelease(config);

    tag(config);
    push();
    pushTags(config);
    // publish(config);

    grunt.verbose.writeln('Post-release, preparing development version');

    bump(config, config.developmentVersion)
    add(config)
    commit(config)
    push();

    function setup(file, type){
      var pkg = grunt.file.readJSON(file);
      var releaseVersion = pkg.version;
      var developmentVersion = semver.inc(pkg.version, type || 'patch');
      if (options.bump) {
        releaseVersion = semver.inc(pkg.version, type || 'patch');
        developmentVersion = semver.inc(releaseVersion, type || 'patch') + "-SNAPSHOT";
      }
      return {file: file, pkg: pkg, releaseVersion: releaseVersion, developmentVersion: developmentVersion };
    }

    // add updated npm package config and maven pom to git
    function add(config){
      run('git add ' + config.file);
      run('git add pom.xml')
    }

    // commit the updated npm package config
    function commit(config){
      var message = grunt.template.process(commitMessage, templateOptions);
      run('git commit -m "'+ message+'"', 'package files committed');
    }

    // updates local pom to match release version
    function prepareMavenRelease(config) {
      mvn("mvn --batch-mode -Dtag=v"+config.releaseVersion+" release:update-versions" +
             " -DreleaseVersion="+config.releaseVersion +
             " -DdevelopmentVersion="+config.developmentVersion +"-SNAPSHOT"
      )
    }

    function performMavenRelease(config) {
      mvn("mvn --batch-mode release:perform")
    }

    function tag(config){
      var name = grunt.template.process(tagName, templateOptions);
      var message = grunt.template.process(tagMessage, templateOptions);
      run('git tag ' + name + ' -m "'+ message +'"', 'New git tag created: ' + name);
    }

    function push(){
      run('git push origin master', 'pushed to remote');
    }

    function pushTags(config){
      run('git push --tags origin', 'pushed new tag '+ config.releaseVersion +' to remote');
    }

    function publish(config) {
      mvn("mvn --batch-mode deploy")
      // var cmd = 'npm publish';
      // var msg = 'published '+ config.releaseVersion +' to npm';
      // var npmtag = getNpmTag();
      // if (npmtag){ 
      //   cmd += ' --tag ' + npmtag;
      //   msg += ' with a tag of "' + npmtag + '"';
      // }
      // if (options.folder){ cmd += ' ' + options.folder }
      // run(cmd, msg);
    }

    function getNpmTag(){
      var tag = grunt.option('npmtag') || options.npmtag;
      if(tag === true) { tag = config.releaseVersion }
      return tag;
    }

    function run(cmd, msg){
      var nowrite = grunt.option('no-write');
      if (nowrite) {
        grunt.verbose.writeln('Not actually running: ' + cmd);
      }
      else {
        grunt.verbose.writeln('Running: ' + cmd);
        var exitCode = shell.exec(cmd, {silent:false}).code;
        if (exitCode != 0) {
          grunt.fail.fatal("[ERROR] " + cmd, exitCode)
        }
      }

      if (msg) grunt.log.ok(msg);
    }


    function mvn(cmd, msg) {
      var nowrite = grunt.option('no-write');
      if (nowrite) {
        cmd = cmd + " -DdryRun=true";
        grunt.verbose.writeln('Maven dry run.');
      }

      grunt.verbose.writeln("[Grunt] " + cmd);

      var exitCode = shell.exec(cmd, {silent:false}).code;

      if (exitCode != 0) {
        grunt.fail.fatal("[ERROR] " + cmd, exitCode)
      }

      if (msg) grunt.log.ok(msg);
    }

    function bump(config, version) {
      config.pkg.version = version
      grunt.file.write(config.file, JSON.stringify(config.pkg, null, '  ') + '\n');
      mvn("mvn versions:set -DnewVersion="+version)
      grunt.log.ok('Versions bumped to ' + version);
    }

  });
};

