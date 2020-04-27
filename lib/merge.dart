import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

const int MERGE_CHUNK_SIZE = 2048;
const int BULK_WRITE_SIZE = 8192;

void runMergeScheduler(SendPort sendPort) async {
  print('Merger started');

  var rc = ReceivePort();

  rc.listen((data) {
    print('Inside isolate: $data');
  });

  sendPort.send(rc.sendPort);

  await merge(1, 2);
}

class Row {
  final String key;
  final Uint8List value;

  Row(this.key, this.value);
}

class TableCursor {
  int tableIndex;
  RandomAccessFile file;
  Uint8List buf;
  ByteData view;
  int fileOffset = 0;
  int offset = 0;
  int chunkLength = null;

  TableCursor(this.tableIndex) {
    buf = Uint8List(MERGE_CHUNK_SIZE);
    view = ByteData.view(buf.buffer);
  }

  Future<void> start() async {
    file = await File('db/table$tableIndex').open(mode: FileMode.read);
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
    var value = buf.sublist(offset + 4 + keyLength, offset + 4 + keyLength + valueLength);

    offset += 4 + keyLength + valueLength;

    return Row(key, value);
  }
}

void merge(int tableIndex1, int tableIndex2) async {
  final List<Row> list = [];

  final t1 = TableCursor(tableIndex1);
  final t2 = TableCursor(tableIndex2);

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

  var table = Uint8List(BULK_WRITE_SIZE);
  var view = ByteData.view(table.buffer);
  var offset = 0;

  var newTable = await File('db/table1_1').openWrite();

  for (var item in list) {
    var keyCodes = item.key.codeUnits;
    var keyLength = keyCodes.length;

    if (offset + 4 + keyLength + item.value.length > table.length) {
      newTable.add(table.sublist(0, offset));
      await newTable.flush();
      table.clear();
      offset = 0;
    }

    view.setUint16(offset, keyCodes.length);
    view.setUint16(offset + 2, item.value.length);
    table.setRange(offset + 4, offset + 4 + keyLength, keyCodes);
    table.setRange(offset + 4 + keyLength, offset + 4 + keyLength + item.value.length, item.value);

    offset += 4 + keyLength + item.value.length;
  }

  newTable.add(table.sublist(0, offset));

  await newTable.flush();
  await newTable.close();

  print('Table "table${tableIndex1}" and "table${tableIndex2}" merged into "table1_1"');
}
