import 'dart:io';
import 'dart:convert';
import 'dart:math';

void main() async {
  var writter = File('ammo.txt').openWrite(mode: FileMode.write);

  writter.write(
'''[Host: example.org]
[User-Agent: Tank]
[Content-type: application/json]
''');

  for (var i = 1; i < 100000; i++) {
    var id = Random().nextInt(20000);

    if (Random().nextInt(10) < 5) {
      var setJson = jsonEncode({
        'key': 'key$id',
        'value': 'some value of key $id by $i iteration, keks! popy',
      });

      writter.write('${setJson.length + 1} /set\n $setJson\n');
    } else {
      var getJson = jsonEncode({
        'key': 'key$id',
      });

      writter.write('${getJson.length + 1} /get\n $getJson\n');
    }

    if (i % 10000 == 0) {
      await writter.flush();
    }

  }

  await writter.flush();
  await writter.close();
}
