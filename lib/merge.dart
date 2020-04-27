import 'dart:convert';
import 'dart:io';
import 'dart:async';
import 'dart:isolate';
import 'dart:typed_data';
import 'dart:math';

import 'isolate_medium.dart';

const int MERGE_CHUNK_SIZE = 2048;
const int BULK_WRITE_SIZE = 8192;

void runMergeScheduler(SendPort sendPort) async {
  print('Merger started');

  final medium = IsolateMedium(null, sendPort: sendPort);

  schedule(medium);
}

void schedule(IsolateMedium medium) {
  Timer(Duration(seconds: 5), () async {
    await run(medium);
    schedule(medium);
  });
}

class Row {
  final String key;
  final Uint8List value;

  Row(this.key, this.value);
}

class TableCursor {
  String pageName;
  RandomAccessFile file;
  Uint8List buf;
  ByteData view;
  int fileOffset = 0;
  int offset = 0;
  int chunkLength = null;

  TableCursor(this.pageName) {
    buf = Uint8List(MERGE_CHUNK_SIZE);
    view = ByteData.view(buf.buffer);
  }

  Future<void> start() async {
    file = await File('db/$pageName').open(mode: FileMode.read);
  }

  Future<bool> readNextChunk() async {
    fileOffset += offset;
    await file.setPosition(fileOffset);
    chunkLength = await file.readInto(buf);
    offset = 0;

    return chunkLength > 0;
  }

  Future<Row> readRow() async {
    if (chunkLength == 0) {
      return null;
    }

    if (chunkLength == null) {
      await readNextChunk();
    }

    if (offset + 4 > chunkLength) {
      if (!(await readNextChunk())) {
        return null;
      }
    }

    var keyLength = view.getUint16(offset);
    var valueLength = view.getUint16(offset + 2);

    if (offset + 4 + keyLength + valueLength < chunkLength) {
      await readNextChunk();
    }

    var key = String.fromCharCodes(buf, offset + 4, offset + 4 + keyLength);
    var value = buf.sublist(
        offset + 4 + keyLength, offset + 4 + keyLength + valueLength);

    offset += 4 + keyLength + valueLength;

    return Row(key, value);
  }
}

void run(IsolateMedium medium) async {
  var jsonData = await File('db/state.json').readAsString();

  var state = jsonDecode(jsonData);
  var pages = List<String>.from(state['pages']);

  if (pages.length >= 2) {
    await merge(pages[0], pages[1], medium: medium);
  }
}

void merge(String pageName1, String pageName2, {IsolateMedium medium}) async {
  final List<Row> list = [];

  final t1 = TableCursor(pageName1);
  final t2 = TableCursor(pageName2);

  await Future.wait([
    t1.start(),
    t2.start(),
  ]);

  var t1value = await t1.readRow();
  var t2value = await t2.readRow();

  while (t1value != null || t2value != null) {
    if (t1value == null) {
      list.add(t2value);
      t2value = await t2.readRow();
      continue;
    }

    if (t2value == null) {
      list.add(t1value);
      t1value = await t1.readRow();
      continue;
    }

    final comparison = t1value.key.compareTo(t2value.key);

    if (comparison == 0) {
      list.add(t2value);
      t1value = await t1.readRow();
      t2value = await t2.readRow();
    } else if (comparison > 0) {
      list.add(t2value);
      t2value = await t2.readRow();
    } else {
      list.add(t1value);
      t1value = await t1.readRow();
    }
  }

  var page = Uint8List(BULK_WRITE_SIZE);
  var view = ByteData.view(page.buffer);
  var offset = 0;

  var index = Uint8List(BULK_WRITE_SIZE);
  var indexView = ByteData.view(index.buffer);
  var indexOffset = 0;

  var newPageName = 'table${Random().nextInt(4294967296)}m';

  var newPage = await File('db/$newPageName').openWrite();
  var newPageIndex = await File('db/${newPageName}_index').openWrite();

  for (var item in list) {
    var keyCodes = item.key.codeUnits;
    var keyLength = keyCodes.length;

    // Flush page data
    if (offset + 4 + keyLength + item.value.length > page.length) {
      newPage.add(page.sublist(0, offset));
      await newPage.flush();
      page.clear();
      offset = 0;
    }

    // Flush index data;
    if (indexOffset + 6 + keyLength > index.length) {
      newPageIndex.add(index.sublist(0, indexOffset));
      await newPageIndex.flush();
      index.clear();
      indexOffset = 0;
    }

    view.setUint16(offset, keyCodes.length);
    view.setUint16(offset + 2, item.value.length);
    page.setRange(offset + 4, offset + 4 + keyLength, keyCodes);
    page.setRange(offset + 4 + keyLength,
        offset + 4 + keyLength + item.value.length, item.value);

    indexView.setUint32(indexOffset, offset);
    indexView.setUint16(indexOffset + 4, keyLength);
    index.setRange(indexOffset + 6, indexOffset + 6 + keyLength, keyCodes);

    offset += 4 + keyLength + item.value.length;
    indexOffset += 6 + keyLength;
  }

  newPage.add(page.sublist(0, offset));
  newPageIndex.add(index.sublist(0, indexOffset));

  await Future.wait([
    () async {
      await newPage.flush();
      await newPage.close();
    }(),
    () async {
      await newPageIndex.flush();
      await newPageIndex.close();
    }()
  ]);

  print('Table "${pageName1}" and "$pageName2" merged into "$newPageName"');

  await medium.call('update_state', {
    'replacePages': [pageName1, pageName2],
    'byPage': newPageName,
  });
}
