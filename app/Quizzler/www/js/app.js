// Ionic Starter App

// angular.module is a global place for creating, registering and retrieving Angular modules
// 'starter' is the name of this angular module example (also set in a <body> attribute in index.html)
// the 2nd parameter is an array of 'requires'
angular.module('Archos', ['ionic']);
angular.module('Archos.TelegramController', []);

var Archos = angular.module('Archos');


Archos.run(function($ionicPlatform, $http) {
    $ionicPlatform.ready(function() {
        if(window.cordova && window.cordova.plugins.Keyboard) {
          // Hide the accessory bar by default (remove this to show the accessory bar above the keyboard
          // for form inputs)
          cordova.plugins.Keyboard.hideKeyboardAccessoryBar(true);

          // Don't remove this line unless you know what you are doing. It stops the viewport
          // from snapping when text inputs are focused. Ionic handles this internally for
          // a much nicer keyboard experience.
          cordova.plugins.Keyboard.disableScroll(true);
        }
        if(window.StatusBar) {
          StatusBar.styleDefault();
        }
    });
});


Archos.controller("QuizzlerController", function($ionicPlatform, $http, $scope, $timeout, $ionicModal) {
    var timer_timeout
    $scope.question = {
      questionText: "",
      questionCategory: "",
      answers: ""
    };
    $scope.list_of_questions = [];

    $scope.onTimeout = function() {
      $scope.currentTime = formatDate(new Date())
      timer_timeout = $timeout($scope.onTimeout, 1000);
    };
    $scope.startTimer = function() {
      timer_timeout = $timeout($scope.onTimeout, 1000);
    };
    function formatDate(date)
    {
      var hours = date.getHours()
      var minutes = date.getMinutes()
      var seconds = date.getSeconds()
      var dd = "AM"
      if(hours >= 12)
      {
        hours = hours - 12
        dd = "PM"
      }
      return hours + ":" + (minutes < 10 ? "0" + minutes : minutes) + ":" + (seconds < 10 ? "0" + seconds : seconds) + " " + dd

    }
    $scope.getQuestions = function()
    {
        $http.get("http://localhost:5000/get_questions").then(function(response){
          angular.forEach(response.data.questions, function(value, key){
            $scope.list_of_questions.push({
                question_id: value.question_id,
                question_text: value.question
              });
          });
        });
    }
    $scope.addQuestion = function()
    {
      $http.get('http://localhost:5000/add_question', {params:{
        'question': $scope.question.questionText,
        'category': $scope.question.questionCategory,
        'answer': $scope.question.answer
      }}).success(function(data){
        $scope.closeModal();
      }).then(function(data){
        $scope.list_of_questions.push($scope.question.questionText);
        $scope.question = {
          questionText: "",
          questionCategory: "",
          answers: ""
        };
      });

    }
    $ionicModal.fromTemplateUrl('add_question.html', {
      scope: $scope,
      animation: 'slide-in-up',
      focusFirstInput: true
    }).then(function(modal) {
      $scope.modal = modal;
    });

    $scope.openModal = function() {
      $scope.modal.show();
    };

    $scope.closeModal = function() {
      $scope.modal.hide();
    };


    $scope.$on('$destroy', function() {
      $scope.modal.remove();
    });

    $scope.$on('modal.hidden', function() {
      // Execute action
    });

    $scope.$on('modal.removed', function() {
      // Execute action
    });
});
