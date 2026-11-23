String? ntfyTopicUrl(String base, String topic, String since, {required bool sse}) {
  final root = base.trim();
  final id = topic.trim();
  final cursor = since.trim();
  if (root.isEmpty || id.isEmpty || cursor.isEmpty) return null;
  final host = root.endsWith('/') ? root.substring(0, root.length - 1) : root;
  final path = sse ? 'sse' : 'json';
  return '$host/${Uri.encodeComponent(id)}/$path?since=${Uri.encodeQueryComponent(cursor)}';
}
