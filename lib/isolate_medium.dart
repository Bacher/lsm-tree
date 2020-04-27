import 'dart:async';
import 'dart:isolate';

typedef MethodHandler = Future<dynamic> Function(
    String methodName, dynamic params);

class SendQueueItem {
  final String methodName;
  final dynamic params;
  final Completer completer = Completer();

  SendQueueItem(this.methodName, this.params);
}

class IsolateMedium {
  SendPort sendPort;
  ReceivePort receiverPort;
  final MethodHandler methodHandler;
  List<SendQueueItem> sendQueue;
  final Map<int, Completer> waits = {};
  int lastId = 0;

  IsolateMedium(this.methodHandler, {this.sendPort}) {
    receiverPort = ReceivePort();
    receiverPort.listen(listen);

    if (sendPort != null) {
      sendPort.send(receiverPort.sendPort);
    }
  }

  void listen(dynamic data) async {
    if (sendPort == null) {
      sendPort = data;

      if (sendQueue != null && sendQueue.isNotEmpty) {
        for (var item in sendQueue) {
          _send(item.methodName, item.params, item.completer);
        }
        sendQueue = null;
      }
      return;
    }

    String type = data['type'];

    switch (type) {
      case 'request':
        String methodName = data['method'];

        dynamic result;

        try {
          result = await methodHandler(methodName, data['params']);
        } catch (err, stack) {
          print('Method handle failed:');
          print(err);
          print(stack);

          sendPort.send({
            'type': 'response',
            'response_for': data['id'],
            'error': {
              'message': 'Error',
            },
          });
        }

        sendPort.send({
          'type': 'response',
          'response_for': data['id'],
          'result': result,
        });
        break;
      case 'response':
        int id = data['response_for'];

        if (data['error'] != null) {
          waits[id].complete(Future.error(data['error']));
        } else {
          waits[id].complete(data['result']);
        }
        break;
      default:
        print('Unknown event: $type');
    }
  }

  SendPort get pipe {
    return receiverPort.sendPort;
  }

  Future<dynamic> call(String methodName, dynamic params) async {
    if (sendPort == null) {
      var sendItem = SendQueueItem(methodName, params);
      sendQueue ??= [];
      sendQueue.add(sendItem);
      return sendItem.completer.future;
    }

    var completer = Completer();

    _send(methodName, params, completer);

    return completer.future;
  }

  void _send(String methodName, dynamic params, Completer completer) {
    var requestId = ++lastId;
    waits[requestId] = completer;

    sendPort.send({
      'type': 'request',
      'id': requestId,
      'method': methodName,
      'params': params,
    });
  }
}
