/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var container = require("vertx/container");
var vertx = require("vertx")
var vertxTests = require("vertx_tests");
var vassert = require("vertx_assert");

var eb = vertx.eventBus;

var numMessages = 10;

function testPersistentReloadWorkQueue() {

  var count = 0;
  var doneHandler = function() {
    if (++count == numMessages) {
      vassert.testComplete();
    }
  };

  eb.registerHandler("done", doneHandler);

  var persistorConfig =
  {
    address: 'test.persistor',
    db_name: java.lang.System.getProperty("vertx.mongo.database", "test_db"),
    host: java.lang.System.getProperty("vertx.mongo.host", "localhost"),
    port: java.lang.Integer.valueOf(java.lang.System.getProperty("vertx.mongo.port", "27017"))
  }
  var username = java.lang.System.getProperty("vertx.mongo.username");
  var password = java.lang.System.getProperty("vertx.mongo.password");
  if (username != null) {
    persistorConfig.username = username;
    persistorConfig.password = password;
  }
  container.deployModule('io.vertx~mod-mongo-persistor~2.0.0-CR2', persistorConfig, function(err, deployID) {
    insertWork(function() {
      var queueConfig = {address: 'test.orderQueue', persistor_address: 'test.persistor', collection: 'work'}
      container.deployModule(java.lang.System.getProperty("vertx.modulename"), queueConfig, function(err, deployID) {
        container.deployVerticle('integration_tests/javascript/order_processor.js', 10);
      });
    });

  });
}

function insertWork(doneHandler) {

  var count = 0;
  for (var i = 0; i < numMessages; i++) {
    eb.send('test.persistor', {
      collection: 'work',
      action: 'save',
      document: {
        blah: "foo" + i
      }
    }, function(reply) {
      vassert.assertEquals('ok', reply.status);
      if (++count == numMessages) {
        doneHandler();
      }
    });
  }
}

vertxTests.startTests(this)