// Karma configuration
// Generated on Wed Sep 18 2013 15:45:48 GMT+0200 (CEST)

module.exports = function(config) {
  config.set({

    preprocessors: {
      'app/scripts/**/*.coffee': ['coffee'],
      'lib/*.coffee': ['coffee'],
      'test/spec/**/*.coffee': ['coffee']
    },

    plugins: [
      'karma-coffee-preprocessor',
      'karma-jasmine',
      'karma-requirejs',
      'karma-phantomjs-launcher',
      'karma-junit-reporter'
    ],
    // global config for SauceLabs
    sauceLabs: {
      username: 'neo4j-account-is-needed',
      accessKey: 'get-this-from-somewhere-else-not-git-managed',
      startConnect: true,
      testName: 'neo4j browser unit tests'
    },

    // define SL browsers
    customLaunchers: {
      sl_safari_mac: {
        base: 'SauceLabs',
        browserName: 'safari',
        platform: 'OS X 10.8'
      },
      sl_chrome_mac: {
        base: 'SauceLabs',
        browserName: 'chrome',
        platform: 'OS X 10.8'
      },
      sl_ie_windows_8: {
        base: 'SauceLabs',
        browserName: 'internet explorer',
        platform: 'Windows 8'
      }
    },

    // base path, that will be used to resolve files and exclude
    basePath: '',


    // frameworks to use
    frameworks: ['jasmine'],


    // list of files / patterns to load in the browser
    files: [
      'app/components/jquery/dist/jquery.js',
      'app/components/angular/angular.js',
      'app/components/angular-ui-bootstrap/src/modal/modal.js',
      'app/components/angular-ui-bootstrap/src/transition/transition.js',
      'app/components/angular-local-storage/dist/angular-local-storage.js',
      'app/components/angular-mocks/angular-mocks.js',
      'app/components/codemirror/lib/codemirror.js',
      'app/components/angular-ui-codemirror/ui-codemirror.js',
      'app/components/d3/d3.js',
      'app/components/UUID.js/src/uuid.js',
      'app/components/angular-base64/angular-base64.min.js',
      'app/scripts/*.coffee',
      'lib/*.coffee',
      'app/scripts/**/*.coffee',
      'test/spec/**/*.coffee'
    ],


    // list of files to exclude
    exclude: [

    ],


    // test results reporter to use
    // possible values: 'dots', 'progress', 'junit', 'growl', 'coverage'
    reporters: ['progress', 'junit'],


    // web server port
    port: 8080,


    // enable / disable colors in the output (reporters and logs)
    colors: true,


    // level of logging
    // possible values: config.LOG_DISABLE || config.LOG_ERROR || config.LOG_WARN || config.LOG_INFO || config.LOG_DEBUG
    logLevel: config.LOG_INFO,


    // enable / disable watching file and executing tests whenever any file changes
    autoWatch: true,


    // Start these browsers, currently available:
    // - Chrome
    // - ChromeCanary
    // - Firefox
    // - Opera
    // - Safari (only Mac)
    // - PhantomJS
    // - IE (only Windows)
    // browsers: ['PhantomJS','sl_safari_mac','sl_ie_windows_8'],
    browsers: ['PhantomJS'],
    // browsers: ['sl_ie_windows_8'],


    // If browser does not capture in given timeout [ms], kill it
    captureTimeout: 5000,


    // Continuous Integration mode
    // if true, it capture browsers, run tests and exit
    singleRun: true
  });
};
