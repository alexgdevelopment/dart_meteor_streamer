// a package to handle Streamer connections, as per https://github.com/RocketChat/meteor-streamer

import 'package:dart_meteor_streamer/dart_meteor.dart';

class EV {
  Map<String, List<Function>> handlers = {};

  void emit(String event, [List<dynamic> args = const []]) {
    handlers[event]?.forEach((handler) {
      print(
          'emitting event: $event from EV with args: $args for handler ${handler.toString()}');
      handler(args);
    });
  }

  num listenerCount(String event) {
    return handlers.containsKey(event) ? handlers[event]!.length : 0;
  }

  void on(String eventName, Function callback) {
    if (!handlers.containsKey(eventName)) {
      handlers[eventName] = <Function>[];
    }
    handlers[eventName]!.add(callback);
    print('setting handler for event $eventName');
  }

  void once(String eventName, Function callback,
      [List<Object> args = const []]) {
    void oneTimeCallback(args) {
      Function.apply(callback, args);
      removeListener(eventName, oneTimeCallback);
    }

    on(eventName, oneTimeCallback);
  }

  void removeListener(String event, Function callback) {
    if (!handlers.containsKey(event)) {
      return;
    }
    handlers[event]!.remove(callback);
  }

  void removeAllListeners(String event) {
    handlers.remove(event);
  }
}

class StreamerCentral extends EV {
  Map<String, Object> instances = {};
  Map<String, DdpClient> connections = {};

  void setupDdpConnection(String name, DdpClient ddpConnection) {
    if (ddpConnection.hasMeteorStreamerEventListeners) {
      print(
          'cancelled setting up ddp connection as a streamer listener as we already have one');
      return;
    }

    // sets up a listener that fires on msg == changed for the streamer that
    // is listening on the specific CollectionName
    ddpConnection.dataStreamController.stream.listen((data) {
      String msgType = data['msg'];
      print('running listener on data stream');
      print(data);
      if (msgType == 'changed') {
        String collectionName = data['collection']; // stream-notify-user
        dynamic fields =
            data['fields']; // eventName: userId/notification + args
        if (fields != null) {
          String eventName = fields['eventName']; // userId/notifications
          List<dynamic> args = fields['args'];
          DdpClient.formatSpecialFieldValues(fields);
          args.insert(0, eventName); // [eventName, {args}]
          emit(collectionName, args);
        }
      }
    })
      ..onError((dynamic error) {
        print(
            'encountered error $error in streamer central, while setting up ddp connection');
      })
      ..onDone(() {});

    storeDdpConnection(name, ddpConnection);
  }

  void storeDdpConnection(String name, DdpClient ddpConnection) {
    ddpConnection.hasMeteorStreamerEventListeners = true;
    connections[name] = ddpConnection;
  }
}

class Streamer extends EV {
  final String name;
  late DdpClient ddpConnection;
  bool useCollection;
  Map<String, SubscriptionHandler> subscriptions = {};

  String get subscriptionName {
    return 'stream-$name';
  }

  // sets up a streamer that listens for 'changed' events for the 'stream-$name' collection
  Streamer(MeteorClient meteorInstance, this.name,
      [this.useCollection = false]) {
    if (meteorInstance.streamerCentral.instances
        .containsKey(subscriptionName)) {
      print(
          'Streamer listener already exists. Tried to create one for $subscriptionName');
      return;
    }

    ddpConnection = meteorInstance.connection;

    meteorInstance.streamerCentral
        .setupDdpConnection(subscriptionName, ddpConnection);

    meteorInstance.streamerCentral.on(subscriptionName, (
        List<dynamic> args) {
      final String eventName = args.removeAt(0);
      print(
          'running listener on StreamerCentral for subscription $subscriptionName and event $eventName');
      if (subscriptions.containsKey(eventName)) {
        print('running if section as well');
        subscriptions[eventName]?.lastMessage = args;
        emit(eventName, args);
      }
    });

    // TODO implement listener for 'reset'
  }

  void stop(String eventName) {
    subscriptions[eventName]?.stop();
  }

  void stopAll() {
    subscriptions.forEach((key, _) {
      removeAllListeners(key);
    });
  }

  void unsubscribe(String eventName) {
    subscriptions.remove(eventName);
    removeAllListeners(eventName);
  }

  Future subscribe(String eventName, List<dynamic> args) {
    return Future(() {
      if (subscriptions.containsKey(eventName)) {
        print(
            'returning due to existing subsciption $eventName on streamer $subscriptionName');
        return;
      }

      args.insert(0, useCollection);
      args.insert(0, eventName);
      var subscription = ddpConnection.subscribe(subscriptionName, args,
          onStop: (err) {
            unsubscribe(eventName);
            throw Exception(err);
          },
          onReady: () => null);
      subscriptions[eventName] = subscription;
      print('just set subscriptions, they are now like: $subscriptions');
    });
  }

  void onReconnect(Function fn) {
    super.on('__reconnect__', fn);
  }

  Object getLastMessageFromEvent(eventName) {
    return subscriptions[eventName]?.lastMessage;
  }

  @override
  void removeAllListeners(eventName) {
    super.removeAllListeners(eventName);
    stop(eventName);
  }

  // removes the passed listener function from the event
  void removeClientListener(String eventName, Function listener) {
    if (listenerCount(eventName) == 1) {
      stop(eventName);
    }
    removeListener(eventName, listener);
  }

  // sets the passed callback as a listener to the eventName
  Future attachListener(
      String eventName, Function callback, List<dynamic> args) {
    super.on(eventName, callback);
    return subscribe(eventName, args);
  }

  Future attachOnceListener(String eventName, [List<dynamic> args = const []]) {
    var callback = args.removeAt(0);
    super.once(eventName, () {
      callback(args);
      if (listenerCount(eventName) == 0) {
        stop(eventName);
      }
    });

    return subscribe(eventName, args);
  }

  // TODO
  // calls a method on the subscription - not properly implemented
  void signalEmit(List<dynamic> args) {
    ddpConnection.call(subscriptionName, args);
  }
}
