/**
 * Created by peyton on 4/19/16.
 */

Archos.controller('TelegramController', function($scope, $http) {

  $scope.phoneNumber = "";
  $scope.sendCode = function()
  {
    $http.get('https://149.154.167.40:443/auth.sendCode', {params:{
      'phone_number': $scope.phoneNumber,
      'sms_type': '0',
      'api_id': '27865',
      'api_hash': 'b72f81391be49a00a5f74ce2f55adbfc',
      'lang_code': 'en',
    }}).success(function(data){
      console.log(data);
    })
  }

});
