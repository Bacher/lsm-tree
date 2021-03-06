import 'dart:convert';
import 'dart:io';
import 'dart:async';
import 'dart:collection';
import 'dart:isolate';
import 'dart:typed_data';
import 'dart:math';
import 'package:archive/archive.dart';
import 'package:simple_bloom_filter/simple_bloom_filter.dart';
import 'isolate_medium.dart';
import 'merge.dart';

const int MEM_TABLE_SIZE_LIMIT = 5000;
const int CHUNK_SIZE = 8192;
const int MAX_KEY_LENGTH = 1000;
const int MAX_VALUE_LENGTH = 65536;

class UserParamException implements Exception {
  String cause;
  UserParamException(this.cause);
}

class Page {
  String pageName;
  bool isLoaded = false;
  Map<String, int> index = null;
  Future<void> indexLoading;
  RandomAccessFile file;
  Future currentReading = Future.value();

  Page(this.pageName);

  void close() {
    if (file != null) {
      file.close().catchError((err) {
        print('Closing failed:');
        print(err);
      });
    }
  }

  Future<void> loadIndex() async {
    if (isLoaded) {
      return;
    }

    if (indexLoading != null) {
      return indexLoading;
    }

    indexLoading = _loadIndex();
    return indexLoading;
  }

  Future<void> _loadIndex() async {
    var indexBytes = await File('db/${pageName}_index').readAsBytes();
    var view = ByteData.view(indexBytes.buffer);

    index = {};

    int offset = 0;

    while (offset < indexBytes.length) {
      var dataOffset = view.getUint32(offset);
      var keyLength = view.getUint16(offset + 4);
      var key =
          String.fromCharCodes(indexBytes, offset + 6, offset + 6 + keyLength);

      index[key] = dataOffset;

      offset += 6 + keyLength;
    }

    isLoaded = true;
    print('Index "$pageName" loaded');
  }

  Future<Uint8List> getValue(String key) async {
    var offset = index[key];

    if (offset == null) {
      return null;
    }

    var data = Uint8List(CHUNK_SIZE);

    await readFromPosition(offset, data);

    var view = ByteData.view(data.buffer);
    var keyLength = view.getUint16(0);
    var dataLength = view.getUint16(2);

    // TODO: remove
    if (keyLength > 1000) {
      print('offset: $offset');
      print('keyLength: $keyLength');
      print('dataLength: $dataLength');
    }

    var foundKey = String.fromCharCodes(data, 4, 4 + keyLength);

    if (foundKey != key) {
      throw Exception('Keys index mismatch');
    }

    if (dataLength > CHUNK_SIZE - 4 - keyLength) {
      var data = Uint8List(4 + keyLength + dataLength);

      await readFromPosition(offset, data);
    }

    return data.sublist(4 + keyLength, 4 + keyLength + dataLength);
  }

  Future<void> readFromPosition(int position, Uint8List buffer) {
    return currentReading =
        currentReading.then((_) => _readFromPosition(position, buffer));
  }

  Future<void> _readFromPosition(int position, Uint8List buffer) async {
    file ??= await File('db/$pageName').open(mode: FileMode.read);

    await file.setPosition(position);
    await file.readInto(buffer);
  }
}

class State {
  static Future<State> load() async {
    var stateFile = File('db/state.json');

    List<Page> pages = [];

    if (await stateFile.exists()) {
      var stateJson = await File('db/state.json').readAsString();
      var stateData = jsonDecode(stateJson);

      for (String pageName in stateData['pages']) {
        pages.add(Page(pageName));
      }
    } else {
      await File('db/state.json').writeAsString(jsonEncode({'pages': []}));
    }

    return State(pages);
  }

  List<Page> pages;
  Future<void> stateSaving;
  Completer stateSavingCompleter;

  State(this.pages);

  Future<void> addPage(String pageName) async {
    var page = Page(pageName);
    await page.loadIndex();
    pages.add(page);

    await _savePagesState();
  }

  Future<void> _savePagesState() async {
    Completer completer;

    if (stateSaving != null) {
      if (stateSavingCompleter == null) {
        completer = stateSavingCompleter = Completer();
        await stateSaving;
      } else {
        return await stateSavingCompleter.future;
      }
    }

    stateSaving = __savePagesState();
    await stateSaving;
    stateSaving = null;

    if (completer != null) {
      completer.complete();
      stateSavingCompleter = null;
    }
  }

  Future<void> __savePagesState() async {
    await File('db/state.json').writeAsString(jsonEncode({
      'pages': List<String>.from(pages.map((page) => page.pageName)),
    }));
  }

  Future<void> loadLastPage() async {
    if (pages.isNotEmpty) {
      await pages.last.loadIndex();
    }
  }

  Future<Uint8List> getValue(key) async {
    if (pages.isEmpty) {
      return null;
    }

    for (var page in pages.reversed) {
      if (!page.isLoaded) {
        await page.loadIndex();
      }

      var value = await page.getValue(key);

      if (value != null) {
        return value;
      }
    }

    return null;
  }

  Future<void> mergePages(List<String> replacePages, String byPage) async {
    int index;

    for (var i = 0; i < pages.length; i++) {
      var page = pages[i];

      if (page.pageName == replacePages[0]) {
        index = i;
        break;
      }
    }

    if (index == null) {
      throw Exception('Bad');
    }

    if (index == pages.length - 1) {
      throw Exception('Bad');
    }

    if (pages[index + 1].pageName != replacePages[1]) {
      throw Exception('Bad');
    }

    var page1 = pages[index];
    var page2 = pages[index + 1];

    Timer(Duration(seconds: 10), () {
      page1.close();
      page2.close();
    });

    var newPage = Page(byPage);
    pages.replaceRange(index, index + 2, [newPage]);

    await _savePagesState();
    await newPage.loadIndex();

    await Future.wait([
      File('db/${replacePages[0]}').delete(),
      File('db/${replacePages[0]}_index').delete(),
      File('db/${replacePages[1]}').delete(),
      File('db/${replacePages[1]}_index').delete(),
    ]);
  }
}

class Database {
  SplayTreeMap<String, Uint8List> memtable;
  IOSink currentLog;
  Completer pageCreatingCompleter;
  State state;
  bool _isMemTableSavingStarted = false;

  Database() {
    memtable = SplayTreeMap<String, Uint8List>();
  }

  void start() async {
    print('Starting...');

    await Directory('db').create();
    state = await State.load();

    await state.loadLastPage();

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

    startMergeIsolate();
  }

  void startMergeIsolate() {
    var medium = IsolateMedium((String methodName, dynamic params) async {
      switch (methodName) {
        case 'update_state':
          await state.mergePages(
            params['replacePages'].cast<String>(),
            params['byPage'],
          );
          return;
        default:
          throw Exception('Unknown method');
      }
    });

    // ignore: unawaited_futures
    Isolate.spawn(runMergeScheduler, medium.pipe).catchError((err) {
      print('Merge isolate failed:');
      print(err);
    });
  }

  void applyLogData(Uint8List logData) {
    var view = ByteData.view(logData.buffer);
    var blockOffset = 0;

    while (blockOffset < logData.length) {
      var keyLength = view.getUint16(blockOffset);
      var valueLength = view.getUint16(blockOffset + 2);

      var key = String.fromCharCodes(
          logData, blockOffset + 4, blockOffset + 4 + keyLength);
      var value = logData.sublist(blockOffset + 4 + keyLength,
          blockOffset + 4 + keyLength + valueLength);

      memtable[key] = value;

      blockOffset += 4 + keyLength + valueLength;
    }
  }

  Future<Uint8List> get(String key) async {
    return memtable[key] ?? await state.getValue(key);
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

    // Wait page creation if in process now
    if (pageCreatingCompleter != null) {
      await pageCreatingCompleter.future;
    }

    currentLog.add(header);
    currentLog.add(keyCodes);
    currentLog.add(value);

    memtable[key] = value;

    checkMemTableLimit();
  }

  void checkMemTableLimit() {
    if (!_isMemTableSavingStarted && memtable.length >= MEM_TABLE_SIZE_LIMIT) {
      _isMemTableSavingStarted = true;
      Timer(Duration(milliseconds: 1), saveMemTableData);
    }
  }

  void saveMemTableData() async {
    pageCreatingCompleter = Completer();

    var pageName = 'table${Random().nextInt(4294967296)}';

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
      snapshot.setRange(
          offset + 4 + keyLength, offset + 4 + keyLength + valueLength, value);

      viewIndex.setUint32(indexOffset, offset);
      viewIndex.setUint16(indexOffset + 4, keyLength);
      index.setRange(indexOffset + 6, indexOffset + 6 + keyLength, keyCodes);

      bloom.add(key);

      offset += 4 + keyLength + valueLength;
      indexOffset += 6 + keyLength;
    }

    await File('db/$pageName').writeAsBytes(snapshot);
    await File('db/${pageName}_index').writeAsBytes(index);

//    var bytesCount = (bloom.bitArray.length / 8).ceil();
//    var bloomList = Uint8List(bytesCount);
//    bloomList.fillRange(0, bytesCount, 0);
//
//    for (var byteIndex = 0; byteIndex < bytesCount; byteIndex++) {
//      int value = 0;
//
//      for (var biteIndex = 0; biteIndex < 8; biteIndex++) {
//        if (bloom.bitArray[byteIndex * 8 + biteIndex]) {
//          value += 1 << biteIndex;
//        }
//      }
//
//      bloomList[byteIndex] = value;
//    }
//
//    await File('db/${pageName}_bloom').writeAsBytes(bloomList);

    print('Table "$pageName" created');

    await currentLog.close();
    currentLog = File('db/log').openWrite(mode: FileMode.writeOnly);
    memtable.clear();
    pageCreatingCompleter.complete();

    print('Mem table cleared');

    await state.addPage(pageName);

    _isMemTableSavingStarted = false;
  }
}

Future<int> calculate() async {
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

Future<void> wait(Duration duration) {
  final completer = Completer();
  Timer(duration, completer.complete);
  return completer.future;
}
