describe('position elements', function () {

  var TargetElMock = function(width, height) {
    this.width = width;
    this.height = height;

    this.prop = function(propName) {
      return propName === 'offsetWidth' ? width : height;
    };
  };

  var $position;

  beforeEach(module('ui.bootstrap.position'));
  beforeEach(inject(function (_$position_) {
    $position = _$position_;
  }));
  beforeEach(function () {
    this.addMatchers({
      toBePositionedAt: function(top, left) {
        this.message = function() {
          return 'Expected "('  + this.actual.top + ', ' + this.actual.left +  ')" to be positioned at (' + top + ', ' + left + ')';
        };

        return this.actual.top == top && this.actual.left == left;
      }
    });
  });


  describe('append-to-body: false', function () {

    beforeEach(function () {
      //mock position info normally queried from the DOM
      $position.position = function() {
        return {
          width: 20,
          height: 20,
          top: 100,
          left: 100
        };
      };
    });

    it('should position element on top-center by default', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'other')).toBePositionedAt(90, 105);
      expect($position.positionElements({}, new TargetElMock(10, 10), 'top')).toBePositionedAt(90, 105);
      expect($position.positionElements({}, new TargetElMock(10, 10), 'top-center')).toBePositionedAt(90, 105);
    });

    it('should position on top-left', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'top-left')).toBePositionedAt(90, 100);
    });

    it('should position on top-right', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'top-right')).toBePositionedAt(90, 120);
    });

    it('should position elements on bottom-center when "bottom" specified', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'bottom')).toBePositionedAt(120, 105);
      expect($position.positionElements({}, new TargetElMock(10, 10), 'bottom-center')).toBePositionedAt(120, 105);
    });

    it('should position elements on bottom-left', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'bottom-left')).toBePositionedAt(120, 100);
    });

    it('should position elements on bottom-right', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'bottom-right')).toBePositionedAt(120, 120);
    });

    it('should position elements on left-center when "left" specified', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'left')).toBePositionedAt(105, 90);
      expect($position.positionElements({}, new TargetElMock(10, 10), 'left-center')).toBePositionedAt(105, 90);
    });

    it('should position elements on left-top when "left-top" specified', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'left-top')).toBePositionedAt(100, 90);
    });

    it('should position elements on left-bottom when "left-bottom" specified', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'left-bottom')).toBePositionedAt(120, 90);
    });

    it('should position elements on right-center when "right" specified', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'right')).toBePositionedAt(105, 120);
      expect($position.positionElements({}, new TargetElMock(10, 10), 'right-center')).toBePositionedAt(105, 120);
    });

    it('should position elements on right-top when "right-top" specified', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'right-top')).toBePositionedAt(100, 120);
    });

    it('should position elements on right-top when "right-top" specified', function () {
      expect($position.positionElements({}, new TargetElMock(10, 10), 'right-bottom')).toBePositionedAt(120, 120);
    });
  });

});