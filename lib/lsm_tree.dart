import 'dart:io';
import 'dart:async';
import 'dart:collection';
import 'dart:typed_data';
import 'package:archive/archive.dart';

const int MEM_TABLE_SIZE_LIMIT = 10;

class Database {
  SplayTreeMap<String, Uint8List> memtable;
  IOSink currentLog;
  bool _isMemTableSavingStarted = false;

  Database() {
    memtable = SplayTreeMap<String, Uint8List>();
  }

  void start() async {
    print('Starting...');

    await Directory('db').create();

    Uint8List logData;

    try {
      logData = await File('db/log').readAsBytes();
    } catch (err) {
      // Do nothing
    }

    if (logData != null && logData.isNotEmpty) {
      applyLogData(logData);
    }

    var logFile = File('db/log');
    currentLog = logFile.openWrite(mode: FileMode.writeOnlyAppend);
  }

  void applyLogData(Uint8List logData) {
    var view = ByteData.view(logData.buffer);
    var blockOffset = 0;

    while (blockOffset < logData.length) {
      var keyLength = view.getUint16(blockOffset);
      var valueLength = view.getUint16(blockOffset + 2);

      var key = String.fromCharCodes(logData, blockOffset + 4, blockOffset + 4 + keyLength);
      var value = logData.sublist(blockOffset + 4 + keyLength, blockOffset + 4 + keyLength + valueLength);

      memtable[key] = value;

      blockOffset += 4 + keyLength + valueLength;
    }
  }

  Uint8List get(String key) {
    return memtable[key];
  }

  void set(String key, Uint8List value) async {
    var header = Uint8List(4);
    var view = ByteData.view(header.buffer);
    var keyCodes = key.codeUnits;

    view.setUint16(0, keyCodes.length);
    view.setUint16(2, value.length);

    currentLog.add(header);
    currentLog.add(keyCodes);
    currentLog.add(value);
    await currentLog.flush();

    memtable[key] = value;

    checkMemTableLimit();

  }

  void checkMemTableLimit() {
    if (!_isMemTableSavingStarted && memtable.length >= MEM_TABLE_SIZE_LIMIT) {
      _isMemTableSavingStarted = true;
      Timer(Duration(milliseconds: 1), saveMemTableData);
    }
  }

  void saveMemTableData() {
    // get memtable snapshot;

    _isMemTableSavingStarted = false;
  }
}

Future<int> calculate () async {
  var file = File('bin/test.txt');

  var a = await file.open(mode: FileMode.read);

  await a.setPosition(5);

  var result = await a.read(100);

  var encoded = GZipEncoder().encode(result);

  print(result.length);
  print(encoded.length);

  var decoded = GZipDecoder().decodeBytes(encoded);

  print(String.fromCharCodes(decoded));

  // print(result);

  // print(String.fromCharCodes(result));


  return 6 * 7;
}
