/*
 * Copyright (c) Resource Repository
 * 2020-2021 NaikSoftware
 */

import 'dart:async';
import 'dart:convert';
import 'dart:developer';

import 'package:hive_flutter/hive_flutter.dart';
import 'package:resource_repository_storage/resource_repository_storage.dart';
import 'package:rxdart/rxdart.dart';
import 'package:synchronized/synchronized.dart';
import 'package:worker_manager/worker_manager.dart';

/// An implementation of [CacheStorage] using Hive as the database.
/// Recommended for small datasets. If you are working with hundreds or
/// thousands of items, see other storages as [ObjectBoxCacheStorage].
class HiveCacheStorage<K, V> implements CacheStorage<K, V> {
  static Future<void>? _openDbTask;
  static final _lock = Lock();
  static final Map<String, Box<BoxCacheEntry>> _boxes = {};

  final String _boxKey;
  final Future<String> Function(V value) _encode;
  final Future<V> Function(String data) _decode;

  /// Create storage with a key. The key is used as the database name.
  HiveCacheStorage(
    this._boxKey, {
    required V Function(dynamic json) decode,
  })  : _encode =
            ((value) => Executor().execute(arg1: value, fun1: _jsonEncode)),
        _decode = ((data) =>
            Executor().execute(arg1: data, fun1: _jsonDecode).then(decode));

  @override
  Future<void> ensureInitialized() => _lock.synchronized(_initHive);

  /// Init isolates pool for faster data serialization. Usually called at startup
  /// in the [main] function.
  static Future<void> warmUp() => Executor().warmUp();

  /// Deletes all data in the storage.
  static Future<void> clearAll() async {
    await _lock.synchronized(_initHive);
    await Hive.deleteFromDisk();
  }

  static Future<void> _initHive() {
    var task = _openDbTask;
    if (task == null) {
      Hive.registerAdapter(CacheEntryAdapter());
      task = _openDbTask = Hive.initFlutter();
    }
    return task;
  }

  Future<Box<BoxCacheEntry>> _ensureBox() => _lock.synchronized(() async {
        Box<BoxCacheEntry>? box = _boxes[_boxKey];
        if (box == null) {
          try {
            await _initHive();
            box = await Hive.openBox(_boxKey);
          } catch (e, trace) {
            log('Open box $_boxKey error', error: e, stackTrace: trace);
            Hive.deleteBoxFromDisk(_boxKey);
            box = await Hive.openBox(_boxKey);
          }
          _boxes[_boxKey] = box;
        }
        return box;
      });

  @override
  Future<void> clear() => _ensureBox().then((box) => box.clear());

  @override
  Future<CacheEntry<V>?> get(K cacheKey) async {
    final boxKey = _resolveBoxKey(cacheKey);
    final cached = (await _ensureBox()).get(boxKey);
    if (cached != null) {
      try {
        final value = await _decode(cached.data);
        return CacheEntry(value, storeTime: cached.storeTime);
      } catch (e, trace) {
        log('On load resource by key: $boxKey', error: e, stackTrace: trace);
      }
    }
    return null;
  }

  String _resolveBoxKey(K cacheKey) => cacheKey.toString();

  @override
  Future<void> put(K cacheKey, V data, {int? storeTime}) async {
    final boxKey = _resolveBoxKey(cacheKey);
    await (await _ensureBox()).put(
      boxKey,
      BoxCacheEntry(
        await _encode(data),
        storeTime: storeTime ?? DateTime.now().millisecondsSinceEpoch,
      ),
    );
  }

  @override
  Future<void> delete(K cacheKey) async {
    final boxKey = _resolveBoxKey(cacheKey);
    await (await _ensureBox()).delete(boxKey);
  }

  @override
  Stream<List<V>> watch() => _ensureBox().asStream().switchMap((box) => box
      .watch()
      .startWith(BoxEvent('First load', null, false))
      .switchMap((event) =>
          Stream.fromFutures(box.values.map((e) => _decode(e.data)))
              .toList()
              .asStream()));

  @override
  Future<List<V>> getAll() async {
    final valuesRaw = await _ensureBox().then((value) => value.values);
    final List<V> values = [];
    for (final value in valuesRaw) {
      values.add(await _decode(value.data));
    }
    return values;
  }

  @override
  String toString() => 'HiveCacheStorage($_boxKey)';
}

/// Internal model stored in the Hive.
class BoxCacheEntry {
  String data;
  int storeTime;

  BoxCacheEntry(this.data, {required this.storeTime});
}

/// Adapter for [BoxCacheEntry].
class CacheEntryAdapter extends TypeAdapter<BoxCacheEntry> {
  @override
  final typeId = 0;

  @override
  BoxCacheEntry read(BinaryReader reader) {
    return BoxCacheEntry(reader.read(), storeTime: reader.read());
  }

  @override
  void write(BinaryWriter writer, BoxCacheEntry obj) {
    writer.write(obj.data);
    writer.write(obj.storeTime);
  }
}

String _jsonEncode<V>(V value, TypeSendPort _) {
  return json.encode(value);
}

dynamic _jsonDecode(String data, TypeSendPort _) {
  return json.decode(data);
}
