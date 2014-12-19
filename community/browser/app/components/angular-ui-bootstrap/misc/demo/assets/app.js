/* global FastClick, smoothScroll */
angular.module('ui.bootstrap.demo', ['ui.bootstrap', 'plunker', 'ngTouch'], function($httpProvider){
  FastClick.attach(document.body);
  delete $httpProvider.defaults.headers.common['X-Requested-With'];
}).run(['$location', function($location){
  //Allows us to navigate to the correct element on initialization
  if ($location.path() !== '' && $location.path() !== '/') {
    smoothScroll(document.getElementById($location.path().substring(1)), 500, function(el) {
      location.replace('#' + el.id);
    });
  }
}]).factory('buildFilesService', function ($http, $q) {

  var moduleMap;
  var rawFiles;

  return {
    getModuleMap: getModuleMap,
    getRawFiles: getRawFiles,
    get: function () {
      return $q.all({
        moduleMap: getModuleMap(),
        rawFiles: getRawFiles(),
      });
    }
  };

  function getModuleMap() {
    return moduleMap ? $q.when(moduleMap) : $http.get('assets/module-mapping.json')
      .then(function (result) {
        moduleMap = result.data;
        return moduleMap;
      });
  }

  function getRawFiles() {
    return rawFiles ? $q.when(rawFiles) : $http.get('assets/raw-files.json')
      .then(function (result) {
        rawFiles = result.data;
        return rawFiles;
      });
  }

});

var builderUrl = "http://50.116.42.77:3001";

function MainCtrl($scope, $http, $document, $modal, orderByFilter) {
  $scope.showBuildModal = function() {
    var modalInstance = $modal.open({
      templateUrl: 'buildModal.html',
      controller: 'SelectModulesCtrl',
      resolve: {
        modules: function(buildFilesService) {
          return buildFilesService.getModuleMap()
            .then(function (moduleMap) {
              return Object.keys(moduleMap);
            });
        }
      }
    });
  };

  $scope.showDownloadModal = function() {
    var modalInstance = $modal.open({
      templateUrl: 'downloadModal.html',
      controller: 'DownloadCtrl'
    });
  };
}

var SelectModulesCtrl = function($scope, $modalInstance, modules, buildFilesService) {
  $scope.selectedModules = [];
  $scope.modules = modules;

  $scope.selectedChanged = function(module, selected) {
    if (selected) {
      $scope.selectedModules.push(module);
    } else {
      $scope.selectedModules.splice($scope.selectedModules.indexOf(module), 1);
    }
  };

  $scope.downloadBuild = function () {
    $modalInstance.close($scope.selectedModules);
  };

  $scope.cancel = function () {
    $modalInstance.dismiss();
  };

  $scope.isOldBrowser = function () {
    return isOldBrowser;
  };

  $scope.build = function (selectedModules, version) {
    /* global JSZip, saveAs */
    var moduleMap, rawFiles;

    buildFilesService.get().then(function (buildFiles) {
      moduleMap = buildFiles.moduleMap;
      rawFiles = buildFiles.rawFiles;

      generateBuild();
    });

    function generateBuild() {
      var srcModuleNames = selectedModules
      .map(function (module) {
        return moduleMap[module];
      })
      .reduce(function (toBuild, module) {
        addIfNotExists(toBuild, module.name);

        module.dependencies.forEach(function (depName) {
          addIfNotExists(toBuild, depName);
        });
        return toBuild;
      }, []);

      var srcModules = srcModuleNames
      .map(function (moduleName) {
        return moduleMap[moduleName];
      });

      var srcModuleFullNames = srcModules
      .map(function (module) {
        return module.moduleName;
      });

      var srcJsContent = srcModules
      .reduce(function (buildFiles, module) {
        return buildFiles.concat(module.srcFiles);
      }, [])
      .map(getFileContent)
      .join('\n')
      ;

      var jsFile = createNoTplFile(srcModuleFullNames, srcJsContent);

      var tplModuleNames = srcModules
      .reduce(function (tplModuleNames, module) {
        return tplModuleNames.concat(module.tplModules);
      }, []);

      var tplJsContent = srcModules
      .reduce(function (buildFiles, module) {
        return buildFiles.concat(module.tpljsFiles);
      }, [])
      .map(getFileContent)
      .join('\n')
      ;

      var jsTplFile = createWithTplFile(srcModuleFullNames, srcJsContent, tplModuleNames, tplJsContent);

      var zip = new JSZip();
      zip.file('ui-bootstrap-custom-' + version + '.js', rawFiles.banner + jsFile);
      zip.file('ui-bootstrap-custom-' + version + '.min.js', rawFiles.banner + uglify(jsFile));
      zip.file('ui-bootstrap-custom-tpls-' + version + '.js', rawFiles.banner + jsTplFile);
      zip.file('ui-bootstrap-custom-tpls-' + version + '.min.js', rawFiles.banner + uglify(jsTplFile));

      saveAs(zip.generate({type: 'blob'}), 'ui-bootstrap-custom-build.zip');
    }

    function createNoTplFile(srcModuleNames, srcJsContent) {
      return 'angular.module("ui.bootstrap", [' + srcModuleNames.join(',') + ']);\n' +
        srcJsContent;
    }

    function createWithTplFile(srcModuleNames, srcJsContent, tplModuleNames, tplJsContent) {
      var depModuleNames = srcModuleNames.slice();
      depModuleNames.unshift('"ui.bootstrap.tpls"');

      return 'angular.module("ui.bootstrap", [' + depModuleNames.join(',') + ']);\n' +
        'angular.module("ui.bootstrap.tpls", [' + tplModuleNames.join(',') + ']);\n' +
        srcJsContent + '\n' + tplJsContent;

    }

    function addIfNotExists(array, element) {
      if (array.indexOf(element) == -1) {
        array.push(element);
      }
    }

    function getFileContent(fileName) {
      return rawFiles.files[fileName];
    }

    function uglify(js) {
      /* global UglifyJS */

      var ast = UglifyJS.parse(js);
      ast.figure_out_scope();

      var compressor = UglifyJS.Compressor();
      var compressedAst = ast.transform(compressor);

      compressedAst.figure_out_scope();
      compressedAst.compute_char_frequency();
      compressedAst.mangle_names();

      var stream = UglifyJS.OutputStream();
      compressedAst.print(stream);

      return stream.toString();
    }
  };
};

var DownloadCtrl = function($scope, $modalInstance) {
  $scope.options = {
    minified: true,
    tpls: true
  };

  $scope.download = function (version) {
    var options = $scope.options;

    var downloadUrl = ['ui-bootstrap-'];
    if (options.tpls) {
      downloadUrl.push('tpls-');
    }
    downloadUrl.push(version);
    if (options.minified) {
      downloadUrl.push('.min');
    }
    downloadUrl.push('.js');

    return downloadUrl.join('');
  };

  $scope.cancel = function () {
    $modalInstance.dismiss();
  };
};

/*
 * The following compatibility check is from:
 *
 * Bootstrap Customizer (http://getbootstrap.com/customize/)
 * Copyright 2011-2014 Twitter, Inc.
 *
 * Licensed under the Creative Commons Attribution 3.0 Unported License. For
 * details, see http://creativecommons.org/licenses/by/3.0/.
 */
var isOldBrowser;
(function () {

    var supportsFile = (window.File && window.FileReader && window.FileList && window.Blob);
    function failback() {
        isOldBrowser = true;
    }
    /**
     * Based on:
     *   Blob Feature Check v1.1.0
     *   https://github.com/ssorallen/blob-feature-check/
     *   License: Public domain (http://unlicense.org)
     */
    var url = window.webkitURL || window.URL; // Safari 6 uses "webkitURL".
    var svg = new Blob(
        ['<svg xmlns=\'http://www.w3.org/2000/svg\'></svg>'],
        { type: 'image/svg+xml;charset=utf-8' }
    );
    var objectUrl = url.createObjectURL(svg);

    if (/^blob:/.exec(objectUrl) === null || !supportsFile) {
      // `URL.createObjectURL` created a URL that started with something other
      // than "blob:", which means it has been polyfilled and is not supported by
      // this browser.
      failback();
    } else {
      angular.element('<img/>')
          .on('load', function () {
              isOldBrowser = false;
          })
          .on('error', failback)
          .attr('src', objectUrl);
    }

  })();