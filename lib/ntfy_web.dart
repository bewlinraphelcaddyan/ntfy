import 'dart:async';
import 'dart:convert';
import 'dart:html' as html;

import 'package:flutter_web_plugins/flutter_web_plugins.dart';

import 'ntfy_platform_interface.dart';
import 'src/ntfy_url.dart';

class NtfyWeb extends NtfyPlatform {
  static void registerWith(Registrar registrar) {
    NtfyPlatform.instance = NtfyWeb();
  }

  final StreamController<String> _controller = StreamController<String>.broadcast();
  html.EventSource? _source;
  int _generation = 0;
  String _since = '';

  @override
  Stream<String> get messages => _controller.stream;

  @override
  Future<void> subscribe(String url, String topic, {String? auth}) async {
    final generation = ++_generation;
    _source?.close();
    _source = null;
    _since = '${DateTime.now().millisecondsSinceEpoch ~/ 1000}';
    unawaited(_loop(generation, url, topic));
  }

  @override
  Future<void> unsubscribe() async {
    _generation++;
    _source?.close();
    _source = null;
  }

  Future<void> _loop(int generation, String url, String topic) async {
    var delaySeconds = 2;
    while (generation == _generation) {
      final target = ntfyTopicUrl(url, topic, _since, sse: true);
      if (target == null) return;
      final dropped = await _open(generation, target);
      _source?.close();
      _source = null;
      if (generation != _generation || !dropped) return;
      await Future<void>.delayed(Duration(seconds: delaySeconds));
      if (generation != _generation) return;
      delaySeconds = delaySeconds * 2 > 30 ? 30 : delaySeconds * 2;
    }
  }

  Future<bool> _open(int generation, String url) async {
    try {
      final source = html.EventSource(url);
      _source = source;
      final dropped = Completer<void>();
      late final StreamSubscription<html.MessageEvent> messages;
      late final StreamSubscription<html.Event> errors;
      messages = source.onMessage.listen((html.MessageEvent event) {
        if (generation != _generation) return;
        final data = event.data;
        if (data is! String) return;
        _emitMessage(data);
      });
      errors = source.onError.listen((_) {
        if (!dropped.isCompleted) dropped.complete();
      });
      await dropped.future;
      await messages.cancel();
      await errors.cancel();
      return true;
    } catch (_) {
      return false;
    }
  }

  void _emitMessage(String line) {
    final raw = line.trim();
    if (raw.isEmpty) return;
    try {
      final decoded = jsonDecode(raw);
      if (decoded is! Map || decoded['event'] != 'message') return;
      final id = decoded['id'];
      if (id is String && id.isNotEmpty) _since = id;
      if (!_controller.isClosed) _controller.add(raw);
    } catch (_) {}
  }
}
