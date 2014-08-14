# bootstrap - [AngularJS](http://angularjs.org/) directives specific to [Bootstrap](http://getbootstrap.com)

***

[![Build Status](https://secure.travis-ci.org/angular-ui/bootstrap.png)](http://travis-ci.org/angular-ui/bootstrap)
[![devDependency Status](https://david-dm.org/angular-ui/bootstrap/dev-status.png?branch=master)](https://david-dm.org/angular-ui/bootstrap#info=devDependencies)
[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/angular-ui/bootstrap/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

## Demo

Do you want to see directives in action? Visit http://angular-ui.github.io/bootstrap/!

## Installation

Installation is easy as angular-ui-bootstrap has minimal dependencies - only the AngularJS and Bootstrap's CSS are required.
After downloading dependencies (or better yet, referencing them from your favourite CDN) you need to download build version of this project. All the files and their purposes are described here: 
https://github.com/angular-ui/bootstrap/tree/gh-pages#build-files
Don't worry, if you are not sure which file to take, opt for `ui-bootstrap-tpls-[version].min.js`.

When you are done downloading all the dependencies and project files the only remaining part is to add dependencies on the `ui.bootstrap` AngularJS module:

```javascript
angular.module('myModule', ['ui.bootstrap']);
```

Project files are also available through your favourite package manager:
* **Bower**: `bower install angular-bootstrap`
* **NuGet**: https://nuget.org/packages/Angular.UI.Bootstrap/

## Supported browsers

Directives from this repository are automatically tested with the following browsers:
* Chrome (stable and canary channel)
* Firefox
* IE 9 and 10
* Opera
* Safari

Modern mobile browsers should work without problems.

**IE 8 is not officially supported at the moment**. This project is run by volunteers and with the current number of commiters
we are not in the position to guarantee IE8 support. If you need support for IE8 we would welcome a contributor who would like to take care about IE8.
Alternatively you could sponsor this project to guarantee IE8 support.

We believe that most of the directives would work OK after:
* including relevant shims (for ES5 we recommend https://github.com/kriskowal/es5-shim)
* taking care of the steps described in http://docs.angularjs.org/guide/ie

We are simply not regularly testing against IE8.

## Project philosophy

### Native, lightweight directives

We are aiming at providing a set of AngularJS directives based on Bootstrap's markup and CSS. The goal is to provide **native AngularJS directives** without any dependency on jQuery or Bootstrap's JavaScript.
It is often better to rewrite an existing JavaScript code and create a new, pure AngularJS directive. Most of the time the resulting directive is smaller as compared to the original JavaScript code size and better integrated into the AngularJS ecosystem.

### Customizability

All the directives in this repository should have their markup externalized as templates (loaded via `templateUrl`). In practice it means that you can **customize directive's markup at will**. One could even imagine providing a non-Bootstrap version of the templates!

### Take what you need and not more

Each directive has its own AngularJS module without any dependencies on other modules or third-party JavaScript code. In practice it means that you can **just grab the code for the directives you need** and you are not obliged to drag the whole repository.

### Quality and stability

Directives should work. All the time and in all browsers. This is why all the directives have a comprehensive suite of unit tests. All the automated tests are executed on each checkin in several browsers: Chrome, ChromeCanary, Firefox, Opera, Safari, IE9.
In fact we are fortunate enough to **benefit from the same testing infrastructure as AngularJS**!

## Support

If you are having problems making some directives work, there are several ways to get help:

* Live help in the IRC (`#angularjs` channel at the `freenode` network). Use this [webchat](https://webchat.freenode.net/) or your own IRC client.
* Ask a question in [stackoverflow](http://stackoverflow.com/) under the [angular-ui-bootstrap](http://stackoverflow.com/questions/tagged/angular-ui-bootstrap) tag.
* Write your question in our [mailing list](https://groups.google.com/forum/#!categories/angular-ui/bootstrap).

Project's issue on GitHub should be used discuss bugs and features.

## Contributing to the project

We are always looking for the quality contributions! Please check the [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution guidelines.

### Development
#### Prepare your environment
* Install [Node.js](http://nodejs.org/) and NPM (should come with)
* Install global dev dependencies: `npm install -g grunt-cli karma`
* Install local dev dependencies: `npm install` while current directory is bootstrap repo

#### Build
* Build the whole project: `grunt` - this will run `lint`, `test`, and `concat` targets
* To build modules, first run `grunt html2js` then `grunt build:module1:module2...:moduleN`

You can generate a custom build, containing only needed modules, from the project's homepage.
Alternatively you can run local Grunt build from the command line and list needed modules as shown below:

```
grunt build:modal:tabs:alert:popover:dropdownToggle:buttons:progressbar
```

Check the Grunt build file for other tasks that are defined for this project.

#### TDD
* Run test: `grunt watch`
 
This will start Karma server and will continuously watch files in the project, executing tests upon every change.

#### Test coverage
Add the `--coverage` option (e.g. `grunt test --coverage`, `grunt watch --coverage`) to see reports on the test coverage. These coverage reports are found in the coverage folder.

### Customize templates

As mentioned directives from this repository have all the markup externalized in templates. You might want to customize default
templates to match your desired look & feel, add new functionality etc.

The easiest way to override an individual template is to use the `<script>` directive:

```javascript
<script id="template/alert/alert.html" type="text/ng-template">
    <div class='alert' ng-class='type && "alert-" + type'>
        <button ng-show='closeable' type='button' class='close' ng-click='close()'>Close</button>
        <div ng-transclude></div>
    </div>
</script>
```

If you want to override more templates it makes sense to store them as individual files and feed the `$templateCache` from those partials.
For people using Grunt as the build tool it can be easily done using the `grunt-html2js` plugin. You can also configure your own template url.
Let's have a look:

Your own template url is `views/partials/ui-bootstrap-tpls/alert/alert.html`.

Add "html2js" task to your Gruntfile
```
html2js: {
  options: {
    base: '.',
    module: 'ui-templates',
    rename: function (modulePath) {
      var moduleName = modulePath.replace('app/views/partials/ui-bootstrap-tpls/', '').replace('.html', '');
      return 'template' + '/' + moduleName + '.html';
    }
  },
  main: {
    src: ['app/views/partials/ui-bootstrap-tpls/**/*.html'],
    dest: '.tmp/ui-templates.js'
  }
}
```

Make sure to load your template.js file
`<script src="/ui-templates.js"></script>`

Inject the `ui-templates` module in your `app.js`
```
angular.module('myApp', [
  'ui.bootstrap',
  'ui-templates'
]);
```

Then it will work fine!

For more information visit: https://github.com/karlgoldstein/grunt-html2js

### Release
* Bump up version number in `package.json`
* Commit the version change with the following message: `chore(release): [version number]`
* tag
* push changes and a tag (`git push --tags`)
* switch to the `gh-pages` branch: `git checkout gh-pages`
* copy content of the dist folder to the main folder
* Commit the version change with the following message: `chore(release): [version number]`
* push changes
* switch back to the `main branch` and modify `package.json` to bump up version for the next iteration
* commit (`chore(release): starting [version number]`) and push
* publish Bower and NuGet packages

Well done! (If you don't like repeating yourself open a PR with a grunt task taking care of the above!)
