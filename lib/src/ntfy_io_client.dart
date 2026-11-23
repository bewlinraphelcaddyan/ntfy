import 'dart:async';
import 'dart:convert';
import 'dart:io';

class NtfyIoClient {
  final StreamController<String> _controller = StreamController<String>.broadcast();
  HttpClient? _client;
  int _generation = 0;
  String _since = '';

  Stream<String> get messages => _controller.stream;

  Future<void> subscribe(String url, String topic, {String? auth}) async {
    final generation = ++_generation;
    _client?.close(force: true);
    _client = null;
    _since = '${DateTime.now().millisecondsSinceEpoch ~/ 1000}';
    unawaited(_loop(generation, url, topic, auth));
  }

  Future<void> unsubscribe() async {
    _generation++;
    _client?.close(force: true);
    _client = null;
  }

  Future<void> _loop(int generation, String url, String topic, String? auth) async {
    var delaySeconds = 2;
    while (generation == _generation) {
      final connected = await _readOnce(generation, url, topic, auth);
      if (generation != _generation) return;
      if (connected) delaySeconds = 2;
      await Future<void>.delayed(Duration(seconds: delaySeconds));
      if (generation != _generation) return;
      delaySeconds = delaySeconds * 2 > 30 ? 30 : delaySeconds * 2;
    }
  }

  Future<bool> _readOnce(int generation, String url, String topic, String? auth) async {
    final target = _topicUrl(url, topic, _since, sse: false);
    if (target == null) return false;
    final client = HttpClient()..connectionTimeout = const Duration(seconds: 20);
    _client = client;
    try {
      final request = await client.getUrl(Uri.parse(target));
      request.headers.set(HttpHeaders.acceptHeader, 'application/json');
      if (auth != null && auth.trim().isNotEmpty) {
        request.headers.set(HttpHeaders.authorizationHeader, auth.trim());
      }
      final response = await request.close();
      if (generation != _generation) return true;
      if (response.statusCode >= 400) {
        await response.drain<void>();
        return false;
      }
      final lines = response.transform(utf8.decoder).transform(const LineSplitter());
      await for (final line in lines) {
        if (generation != _generation) return true;
        _emitMessage(line);
      }
      return true;
    } catch (_) {
      return false;
    } finally {
      client.close(force: true);
      if (identical(_client, client)) _client = null;
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

String? _topicUrl(String base, String topic, String since, {required bool sse}) {
  final root = base.trim();
  final id = topic.trim();
  final cursor = since.trim();
  if (root.isEmpty || id.isEmpty || cursor.isEmpty) return null;
  final host = root.endsWith('/') ? root.substring(0, root.length - 1) : root;
  final path = sse ? 'sse' : 'json';
  return '$host/${Uri.encodeComponent(id)}/$path?since=${Uri.encodeQueryComponent(cursor)}';
}
