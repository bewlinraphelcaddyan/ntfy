import 'ntfy_platform_interface.dart';
import 'src/ntfy_io_client.dart';

class NtfyWindows extends NtfyPlatform {
  NtfyWindows() : _client = NtfyIoClient();

  static void registerWith() {
    NtfyPlatform.instance = NtfyWindows();
  }

  final NtfyIoClient _client;

  @override
  Future<void> subscribe(String url, String topic, {String? auth}) {
    return _client.subscribe(url, topic, auth: auth);
  }

  @override
  Future<void> unsubscribe() => _client.unsubscribe();

  @override
  Stream<String> get messages => _client.messages;
}
