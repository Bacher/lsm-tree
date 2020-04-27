import 'dart:convert' as convert;
import 'dart:typed_data';
import 'package:shelf/shelf.dart' as shelf;
import 'package:shelf/shelf_io.dart' as io;
import 'lsm_tree.dart';

void startServer({String hostname, int port, Database db}) async {
  var handler = const shelf.Pipeline()
      // .addMiddleware(shelf.logRequests())
      .addHandler((req) => _echoRequest(req, db));

  var server = await io.serve(handler, hostname, port);
  print('Serving at http://${server.address.host}:${server.port}');
}

Future<shelf.Response> _echoRequest(shelf.Request request, Database db) async {
  try {
    return await __echoRequest(request, db);
  } catch (err, stack) {
    print('Request failed with error:');
    print(err);
    print(stack);
    return shelf.Response.internalServerError();
  }
}

Future<shelf.Response> __echoRequest(shelf.Request request, Database db) async {
  if (request.method != 'POST') {
    return shelf.Response.forbidden('Not allowed');
  }

  if (!request.headers['content-type'].startsWith('application/json')) {
    return shelf.Response(400, body: 'Only json allowed');
  }

  var requestBody;

  try {
    requestBody = convert.jsonDecode(await request.readAsString());
  } catch (err) {
    return shelf.Response(400, body: 'Invalid body');
  }

  try {
    return await __handleApi(request.url.toString(), requestBody, db);
  } on UserParamException catch (err) {
    return shelf.Response(400, body: 'Error: ${err.cause}');
  }
}

Future<shelf.Response> __handleApi(
    String apiName, Map<String, dynamic> data, Database db) async {
  String result;

  switch (apiName) {
    case 'set':
      String key = data['key'];
      String value = data['value'];

      var bytes = Uint8List.fromList(value.codeUnits);

      await db.set(key, bytes);

      return shelf.Response.ok(convert.jsonEncode({
        'key': key,
        'value': value,
      }));
    case 'get':
      String key = data['key'];
      var value = await db.get(key);
      result = value == null ? null : String.fromCharCodes(value);

      return shelf.Response.ok(convert.jsonEncode({
        'key': key,
        'value': result,
      }));
    default:
      return shelf.Response.notFound('Method not found');
  }
}
