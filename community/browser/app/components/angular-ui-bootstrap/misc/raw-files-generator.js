/*!
 * Forked from:
 * Bootstrap Grunt task for generating raw-files.min.js for the Customizer
 * http://getbootstrap.com
 * Copyright 2014 Twitter, Inc.
 * Licensed under MIT (https://github.com/twbs/bootstrap/blob/master/LICENSE)
 */

/* jshint node: true */

'use strict';
var fs = require('fs');

function getFiles(filePaths) {
  var files = {};
  filePaths
    .forEach(function (path) {
      files[path] = fs.readFileSync(path, 'utf8');
    });
  return files;
}

module.exports = function generateRawFilesJs(grunt, jsFilename, files, banner) {
  if (!banner) {
    banner = '';
  }

  var filesJsObject = {
    banner: banner,
    files: getFiles(files),
  };

  var filesJsContent = JSON.stringify(filesJsObject);
  try {
    fs.writeFileSync(jsFilename, filesJsContent);
  }
  catch (err) {
    grunt.fail.warn(err);
  }
  grunt.log.writeln('File ' + jsFilename.cyan + ' created.');
};
