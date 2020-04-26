import 'dart:ffi';
import 'dart:io';
import 'dart:async';
import 'dart:collection';
import 'dart:typed_data';
import 'package:archive/archive.dart';
import 'package:simple_bloom_filter/simple_bloom_filter.dart';

const int MEM_TABLE_SIZE_LIMIT = 10;
const int CHUNK_SIZE = 8192;
const int MAX_KEY_LENGTH = 1000;
const int MAX_VALUE_LENGTH = 65536;

class UserParamException implements Exception {
  String cause;
  UserParamException(this.cause);
}

class Index {
  int tableIndex;
  Map<String, int> index = {};

  Index(this.tableIndex);

  Future<Uint8List> getValue(String key) async {
    var offset = index[key];

    if (offset == null) {
      return null;
    }

    var file = await File('db/table$tableIndex').open(mode:FileMode.read);

    var data = Uint8List(CHUNK_SIZE);

    await file.setPosition(offset);
    await file.readInto(data);

    var view = ByteData.view(data.buffer);
    var keyLength = view.getUint16(0);
    var dataLength = view.getUint16(2);

    var foundKey = String.fromCharCodes(data, 4, 4 + keyLength);

    if (foundKey != key) {
      throw Exception('Keys index mismatch');
    }

    if (dataLength > CHUNK_SIZE - 4 - keyLength) {
      var data = Uint8List(4 + keyLength + dataLength);

      await file.setPosition(offset);
      await file.readInto(data);

      // TODO: Do we need check keys again?
    }

    return data.sublist(4 + keyLength, 4 + keyLength + dataLength);
  }
}

class Database {
  SplayTreeMap<String, Uint8List> memtable;
  IOSink currentLog;
  bool _isMemTableSavingStarted = false;
  List<Index> indexes = [];

  Database() {
    memtable = SplayTreeMap<String, Uint8List>();
  }

  void start() async {
    print('Starting...');

    await Directory('db').create();

    var index = await getLastTableIndex();

    if (index != null) {
      await readIndex(index);
    }

    Uint8List logData;

    try {
      logData = await File('db/log').readAsBytes();
    } catch (err) {
      // Do nothing
    }

    if (logData != null && logData.isNotEmpty) {
      applyLogData(logData);
    }

    currentLog = File('db/log').openWrite(mode: FileMode.writeOnlyAppend);

    print('Database started');
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

  Future<Uint8List> get(String key) async {
    var value = memtable[key];

    if (value != null) {
      return value;
    }

    if (indexes.isEmpty) {
      return null;
    }

    for (var index in indexes) {
      var value = await index.getValue(key);

      if (value != null) {
        return value;
      }
    }

    while (indexes.last.tableIndex != 1) {
      var index = await readIndex(indexes.last.tableIndex - 1);

      var value = await index.getValue(key);

      if (value != null) {
        return value;
      }
    }

    return null;
  }

  void set(String key, Uint8List value) async {
    var keyCodes = key.codeUnits;

    if (keyCodes.length > MAX_KEY_LENGTH) {
      throw UserParamException('Key too big');
    }

    if (value.length > MAX_VALUE_LENGTH) {
      throw UserParamException('Value too big');
    }

    var header = Uint8List(4);
    var view = ByteData.view(header.buffer);


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

  Future<int> getLastTableIndex() async {
    var filesList = Directory('db').list();
    var rx = RegExp(r'^db/table(\d+)$');
    int latestTableIndex;

    for (var item in await filesList.toList()) {
      var match = rx.firstMatch(item.path);

      if (match != null) {
        var tableIndex = int.parse(match.group(1));

        if (tableIndex > (latestTableIndex ?? -1)) {
          latestTableIndex = tableIndex;
        }
      }
    }

    return latestTableIndex;
  }

  void saveMemTableData() async {
    var lastTableIndex = await getLastTableIndex();
    var tableIndex = (lastTableIndex ?? 0) + 1;
    var tableName = 'table${tableIndex}';

    var snapshotSize = 0;
    var indexSize = 0;

    for (var key in memtable.keys) {
      var keyCodes = key.codeUnits;

      snapshotSize += 4 + keyCodes.length + memtable[key].length;
      indexSize += 6 + keyCodes.length;
    }

    var snapshot = Uint8List(snapshotSize);
    var view = ByteData.view(snapshot.buffer);
    var index = Uint8List(indexSize);
    var viewIndex = ByteData.view(index.buffer);
    var bloom = simple_bloom_filter(8192, 3);

    int offset = 0;
    int indexOffset = 0;

    for (var key in memtable.keys) {
      var value = memtable[key];
      var keyCodes = key.codeUnits;
      var keyLength = keyCodes.length;
      var valueLength = value.length;

      view.setUint16(offset, keyLength);
      view.setUint16(offset + 2, valueLength);
      snapshot.setRange(offset + 4, offset + 4 + keyLength, keyCodes);
      snapshot.setRange(offset + 4 + keyLength, offset + 4 + keyLength + valueLength, value);

      viewIndex.setUint32(indexOffset, offset);
      viewIndex.setUint16(indexOffset + 4, keyLength);
      index.setRange(indexOffset + 6, indexOffset + 6 + keyLength, keyCodes);

      bloom.add(key);

      offset += 4 + keyLength + valueLength;
      indexOffset += 6 + keyLength;
    }

    await File('db/$tableName').writeAsBytes(snapshot);
    await File('db/${tableName}_index').writeAsBytes(index);


    var bytesCount = (bloom.bitArray.length / 8).ceil();
    var bloomList = Uint8List(bytesCount);
    bloomList.fillRange(0, bytesCount, 0);

    for (var byteIndex = 0; byteIndex < bytesCount; byteIndex++) {
      int value = 0;

      for (var biteIndex = 0; biteIndex < 8; biteIndex++) {
        if (bloom.bitArray[byteIndex * 8 + biteIndex]) {
          value += 1 << biteIndex;
        }
      }

      bloomList[byteIndex] = value;
    }

    await File('db/${tableName}_bloom').writeAsBytes(bloomList);

    print('Table "$tableName" created');

    await readIndex(tableIndex, isLast: true);

    await currentLog.close();
    // await File('db/log').delete();
    currentLog = File('db/log').openWrite(mode: FileMode.writeOnly);
    memtable.clear();

    print('Mem tabled cleared');

    _isMemTableSavingStarted = false;
  }

  Future<Index> readIndex(int tableIndex, { bool isLast = false }) async {
    var indexBytes = await File('db/table${tableIndex}_index').readAsBytes();
    var view = ByteData.view(indexBytes.buffer);

    var index = Index(tableIndex);

    int offset = 0;

    while (offset < indexBytes.length) {
      var dataOffset = view.getUint32(offset);
      var keyLength = view.getUint16(offset + 4);
      var key = String.fromCharCodes(indexBytes, offset + 6, offset + 6 + keyLength);

      index.index[key] = dataOffset;

      offset += 6 + keyLength;
    }

    if (isLast) {
      indexes.insert(0, index);
    } else {
      indexes.add(index);
    }

    print('Index "table$tableIndex" loaded');

    return index;
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
