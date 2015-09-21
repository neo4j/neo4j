/*global module:false*/
module.exports = function(grunt) {

  var packageJSON = grunt.file.readJSON('package.json');
  var bumpFiles = ["package.json", "bower.json", "composer.json"];
  var commitFiles = bumpFiles.concat(["./dist/*"]);

  // Project configuration.
  grunt.initConfig({
    // Metadata.
    pkg: packageJSON,
    header: {
      dist: {
        options: {
          text: "/*! =======================================================\n                      VERSION  <%= pkg.version %>              \n========================================================= */"
        },
        files: {
          '<%= pkg.gruntConfig.dist.js %>': '<%= pkg.gruntConfig.temp.js %>',
          '<%= pkg.gruntConfig.dist.css %>': '<%= pkg.gruntConfig.temp.css %>',
          '<%= pkg.gruntConfig.dist.cssMin %>': '<%= pkg.gruntConfig.temp.cssMin %>'
        }
      }
    },
    // Task configuration.
    uglify: {
      options: {
        preserveComments: 'some'
      },
      dist: {
        src: '<%= pkg.main %>',
        dest: '<%= pkg.gruntConfig.temp.js %>'
      }
    },
    jshint: {
      ignore_warning: {
        options: {
          '-W099': true
        },
        src: '<%= pkg.main %>'
      },
      options: {
        curly: true,
        eqeqeq: true,
        immed: true,
        latedef: false,
        newcap: true,
        noarg: true,
        sub: true,
        undef: true,
        unused: true,
        boss: true,
        eqnull: true,
        browser: true,
        globals: {
          $ : true,
          Modernizr : true,
          console: true,
          define: true,
          module: true,
          require: true
        },
        "-W099": true,
      },
      gruntfile: {
        src: 'Gruntfile.js'
      },
      js: {
        src: '<%= pkg.main %>'
      },
      spec : {
        src: '<%= pkg.gruntConfig.spec %>',
        options : {
          globals : {
            document: true,
            console: false,
            Slider: false,
            $: false,
            jQuery: false,
            _: false,
            _V_: false,
            afterEach: false,
            beforeEach: false,
            confirm: false,
            context: false,
            describe: false,
            expect: false,
            it: false,
            jasmine: false,
            JSHINT: false,
            mostRecentAjaxRequest: false,
            qq: false,
            runs: false,
            spyOn: false,
            spyOnEvent: false,
            waitsFor: false,
            xdescribe: false
          }
        }
      }
    },
    jasmine : {
      src : '<%= pkg.main %>',
      options : {
        specs : '<%= pkg.gruntConfig.spec %>',
        vendor : ['<%= pkg.gruntConfig.js.jquery %>', '<%= pkg.gruntConfig.js.bindPolyfill %>'],
        styles : ['<%= pkg.gruntConfig.css.bootstrap %>', '<%= pkg.gruntConfig.css.slider %>'],
        template : '<%= pkg.gruntConfig.tpl.SpecRunner %>'
      }
    },
    template : {
      'generate-index-page' : {
        options : {
          data : {
            js : {
              modernizr : '<%= pkg.gruntConfig.js.modernizr %>',
              jquery : '<%= pkg.gruntConfig.js.jquery %>',
              slider : '<%= pkg.main %>'
            },
            css : {
              bootstrap : '<%= pkg.gruntConfig.css.bootstrap %>',
              slider : '<%= pkg.gruntConfig.css.slider %>'
            }
          }
        },
        files : {
          'index.html' : ['<%= pkg.gruntConfig.tpl.index %>']
        }
      }
    },
    watch: {
      options : {
        livereload: true
      },
      js : {
        files: '<%= pkg.main %>',
        tasks: ['jshint:js', 'jasmine']
      },
      gruntfile : {
        files: '<%= jshint.gruntfile %>',
        tasks: ['jshint:gruntfile']
      },
      spec : {
        files: '<%= pkg.gruntConfig.spec %>',
        tasks: ['jshint:spec', 'jasmine:src']
      },
      css : {
        files: ['<%= pkg.gruntConfig.less.slider %>', '<%= pkg.gruntConfig.less.rules %>', '<%= pkg.gruntConfig.less.variables %>'],
        tasks: ['less:development']
      },
      index : {
        files: '<%= pkg.gruntConfig.tpl.index %>',
        tasks: ['template:generate-index-page']
      }
    },
    connect: {
      server: {
        options: {
          port: "<%= pkg.gruntConfig.devPort %>"
        }
      }
    },
    open : {
      development : {
        path: 'http://localhost:<%= connect.server.options.port %>'
      }
    },
    less: {
      options: {
        paths: ["bower_components/bootstrap/less"]
      },
      development: {
        files: {
          '<%= pkg.gruntConfig.css.slider %>': '<%= pkg.gruntConfig.less.slider %>'
        }
      },
      production: {
        files: {
         '<%= pkg.gruntConfig.temp.css %>': '<%= pkg.gruntConfig.less.slider %>',
        }
      },
      "production-min": {
        options: {
          yuicompress: true
        },
        files: {
         '<%= pkg.gruntConfig.temp.cssMin %>': '<%= pkg.gruntConfig.less.slider %>'
        }
      }
    },
    clean: {
      dist: ["dist"],
      temp: ["temp"]
    },
    bump: {
      options: {
        files: bumpFiles,
        updateConfigs: [],
        commit: true,
        commitMessage: 'Release v%VERSION%',
        commitFiles: commitFiles,
        createTag: true,
        tagName: 'v%VERSION%',
        tagMessage: 'Version %VERSION%',
        push: false,
        pushTo: 'origin'
      }
    }
  });


  // These plugins provide necessary tasks.
  grunt.loadNpmTasks('grunt-contrib-uglify');
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-contrib-jasmine');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-connect');
  grunt.loadNpmTasks('grunt-contrib-clean');
  grunt.loadNpmTasks('grunt-contrib-less');
  grunt.loadNpmTasks('grunt-open');
  grunt.loadNpmTasks('grunt-template');
  grunt.loadNpmTasks('grunt-header');
  grunt.loadNpmTasks('grunt-bump');

  // Create custom tasks
  grunt.registerTask('test', ['jshint', 'jasmine']);
  grunt.registerTask('build', ['less:development', 'test', 'template']);
  grunt.registerTask('development', ['template', 'connect', 'open:development', 'watch']);
  grunt.registerTask('append-header', ['header', 'clean:temp']);
  grunt.registerTask('production', ['less:production', 'less:production-min', 'test', 'uglify', "clean:dist", 'append-header']);
  grunt.registerTask('dev', 'development');
  grunt.registerTask('prod', 'production');
  grunt.registerTask('dist', 'production');
  grunt.registerTask('dist-no-tests', ['less:production', 'less:production-min', 'uglify', 'append-header']);
  grunt.registerTask('default', 'build');
};
