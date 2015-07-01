###!
Copyright (c) 2002-2015 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

"use strict"

mountFolder = (connect, dir) ->
  connect.static require("path").resolve(dir)
proxySnippet = require('grunt-connect-proxy/lib/utils').proxyRequest;

module.exports = (grunt) ->

  grunt.registerMultiTask "append", "Append specified header to source files if it doesn't exists", ->
    data = @data
    path = require("path")
    files = grunt.file.expand(@filesSrc)
    header = grunt.file.read(grunt.template.process(data.header))
    sep = "\n"
    files.forEach (f) ->
      contents = grunt.file.read(f)
      if contents.indexOf(header) isnt 0
        grunt.file.write f, header + sep + contents
        grunt.log.writeln "Header appended to \"" + f + "\"."

  grunt.registerMultiTask "replace", "Replaces text in files", ->
    data = @data
    path = require("path")
    files = grunt.file.expand(@filesSrc)
    files.forEach (f) ->
      contents = grunt.file.read(f)
      grunt.file.write f, contents.replace(data.find, data.replace)

  # configurable paths
  yeomanConfig =
    app: "app"
    lib: "lib"
    dist: "dist/browser"

  try
    yeomanConfig.app = require("./component.json").appPath or yeomanConfig.app



  #Start grunt config
  grunt.initConfig

    yeoman: yeomanConfig

    append:
      coffee:
        header: "copyright/copyright.coffee"
        src: [
          "<%= yeoman.app %>/scripts/{,*/}*.coffee"
        ]

    watch:
      coffee:
        files: ["<%= yeoman.app %>/scripts/{,*/}*.coffee", "<%= yeoman.lib %>/visualization/**/*.coffee", "<%= yeoman.lib %>/*.coffee"]
        tasks: ["coffee:dist", "coffee:visualization", "coffee:lib"]
      coffeeTest:
        files: ["test/spec/{,*/}*.coffee"]
        tasks: ["coffee:test"]
      stylus:
        files: 'app/styles/*.styl',
        tasks: ["stylus"]
      livereload:
        files: ["<%= yeoman.app %>/{,*/}*.html", "{.tmp,<%= yeoman.app %>}/styles/{,*/}*.css", "{.tmp,<%= yeoman.app %>}/scripts/{,*/}*.js", "<%= yeoman.app %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}"]
        options:
          livereload: true
      jade:
        files: ['app/index.jade', 'app/views/**/*.jade', 'app/content/**/*.jade']
        tasks: ['jade']

    connect:
      options:
        port: 9000
        hostname: "0.0.0.0"
      dist:
        options:
          port: 9001
          middleware: (connect) ->
            [proxySnippet, mountFolder(connect, yeomanConfig.dist)]
      livereload:
        options:
          middleware: (connect) ->
            [proxySnippet, require('connect-livereload')(), mountFolder(connect, ".tmp"), mountFolder(connect, yeomanConfig.app)]
      test:
        options:
          port: 9000
          middleware: (connect) ->
            [mountFolder(connect, ".tmp"), mountFolder(connect, "test")]
      proxies: [
          {
              context: '/db',
              host: 'localhost',
              port: 7474,
              https: false,
              changeOrigin: false
          },
          {
              context: '/user',
              host: 'localhost',
              port: 7474,
              https: false,
              changeOrigin: false
          }
      ]

    open:
      server:
        url: "http://localhost:<%= connect.options.port %>"

    clean:
      dist:
        files: [
          dot: true
          src: [".tmp", "<%= yeoman.dist %>/*",
            "!<%= yeoman.dist %>/.git*",
            "<%= yeoman.app %>/views/**/*.html",
            "<%= yeoman.app %>/content/**/*.html"
          ]
        ]
      server: ".tmp"

    jshint:
      options:
        jshintrc: ".jshintrc"
      all: ["Gruntfile.js", "<%= yeoman.app %>/scripts/{,*/}*.js"]

    karma:
      unit:
        configFile: "karma.conf.js"
        singleRun: true

    coffee:
      options:
        force: true
        bare: true
      dist:
        files: [
          expand: true
          cwd: "<%= yeoman.app %>/scripts"
          src: "{,*/}*.coffee"
          dest: ".tmp/scripts"
          ext: ".js"
        ]
      test:
        files: [
          expand: true
          cwd: "test/spec"
          src: "{,*/}*.coffee"
          dest: ".tmp/spec"
          ext: ".js"
        ]
      visualization:
        files: [
          '.tmp/lib/visualization/neod3.js': [
            '<%= yeoman.lib %>/visualization/neod3.coffee'
            '<%= yeoman.lib %>/visualization/components/*.coffee'
            '<%= yeoman.lib %>/visualization/utils/*.coffee'
            '<%= yeoman.lib %>/visualization/init.coffee'
          ]
        ]
      lib:
        files: [
          expand: true
          cwd: "<%= yeoman.lib %>"
          src: "*.coffee"
          dest: ".tmp/lib"
          ext: ".js"
        ]

    stylus:
      compile:
        files:
          '<%= yeoman.app %>/styles/main.css': ['<%= yeoman.app %>/styles/*.styl']
      options:
        paths: ["<%= yeoman.app %>/vendor/foundation", "<%= yeoman.app %>/images"]

    jade:
      index:
        src: ["<%= yeoman.app %>/index.jade"]
        dest: '<%= yeoman.app %>'
        options:
          client: false
          pretty: true
      html:
        src: ["<%= yeoman.app %>/views/**/*.jade"]
        dest: "<%= yeoman.app %>/views"
        options:
          client: false
          pretty: true
          basePath: "<%= yeoman.app %>/views/"
      content:
        src: ["<%= yeoman.app %>/content/**/*.jade"]
        dest: "<%= yeoman.app %>/content"
        options:
          client: false
          pretty: true
          basePath: "<%= yeoman.app %>/content/"

    concat:
      dist:
        files: {}

    useminPrepare:
      html: "<%= yeoman.app %>/index.html"
      options:
        dest: "<%= yeoman.dist %>"

    usemin:
      html: ["<%= yeoman.dist %>/**/*.html"]
      css: ["<%= yeoman.dist %>/styles/{,*/}*.css"]
      options:
        dirs: ["<%= yeoman.dist %>"]

    imagemin:
      dist:
        files: [
          expand: true
          cwd: "<%= yeoman.app %>/images"
          src: "**/*.{png,jpg,jpeg,ico}"
          dest: "<%= yeoman.dist %>/images"
        ]

    htmlmin:
      dist:
        options: {}
        files: [
          expand: true
          cwd: "<%= yeoman.app %>"
          src: ["*.html", "views/**/*.html", "content/**/*.html"]
          dest: "<%= yeoman.dist %>"
        ]

    uglify:
      options:
        mangle: false
        ASCIIOnly: true

    rev:
      dist:
        files:
          src: ["<%= yeoman.dist %>/scripts/{,*/}*.js", "<%= yeoman.dist %>/styles/{,*/}*.css"]

    copy:
      dist:
        files: [{
            expand: true
            dot: true
            cwd: "<%= yeoman.app %>"
            dest: "<%= yeoman.dist %>"
            src: ["*.{ico,txt}", "images/{,*/}*.{gif,svg,webp}", "fonts/*"]
          },
          {
            expand: true
            flatten: true
            dot: true
            cwd: "<%= yeoman.app %>"
            dest: "<%= yeoman.dist %>/fonts"
            src: ["components/**/*.{otf,woff,ttf,svg}"]
        }]
    shell:
      dirListing:
        command: 'ls',
        options:
            stdout: true

    replace:
      dist:
        find: 'url(/images'
        replace: 'url(/browser/images'
        src: ["<%= yeoman.dist %>/styles/main.css"]

    exec:
      csv_test_prep:
        command: 'mkdir -p target/test-classes/ && cat .tmp/lib/helpers.js .tmp/lib/serializer.js src/test/javascript/prepareCSVTest.js >target/test-classes/CsvExportImportRoundTripTest.js'

  # load all grunt tasks
  require("matchdep").filterDev("grunt-*").forEach grunt.loadNpmTasks

  grunt.registerTask "server", ["clean:server", "coffee", "configureProxies", "stylus", "jade", "connect:livereload", "watch"]
  grunt.registerTask "test", ["clean:server", "coffee", "connect:test", "karma", "exec:csv_test_prep"]
  grunt.registerTask "build", ["clean:dist", "coffee", "test", "jade", "stylus", "useminPrepare", "concat", "copy", "imagemin", "cssmin", "htmlmin", "uglify", "rev", "usemin", "replace"]
  grunt.registerTask "server:dist", ["build", "configureProxies", "connect:dist:keepalive"]
  grunt.registerTask "default", ["build"]

  grunt.task.loadTasks "tasks"
