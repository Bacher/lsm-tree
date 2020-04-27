import 'dart:io';

import 'package:args/args.dart';
import 'package:lsm_tree/server.dart';
import 'package:lsm_tree/lsm_tree.dart';

const _hostname = '0.0.0.0';

void main(List<String> args) async {
  var parser = ArgParser()
    ..addOption('port', abbr: 'p');
  var result = parser.parse(args);

  // For Google Cloud Run, we respect the PORT environment variable
  var portStr = result['port'] ?? Platform.environment['PORT'] ?? '8080';
  var port = int.tryParse(portStr);

  if (port == null) {
    stdout.writeln('Could not parse port value "$portStr" into a number.');
    // 64: command line usage error
    exitCode = 64;
    return;
  }

  var db = Database();
  await db.start();

  try {
    await startServer(hostname: _hostname, port: port, db: db);
  } on Exception catch (err) {
    print(err.toString());
    exit(1);
  }
}
