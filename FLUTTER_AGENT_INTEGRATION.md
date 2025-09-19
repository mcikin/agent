# Flutter App Integration for /agent Commands

This guide shows how to integrate the new `/agent create` and other agent commands into your Flutter app's chat interface.

## Overview

The `/agent` commands provide a natural language interface for DAG (Agent) management:

- `/agent create [description]` - Create new DAGs from natural language
- `/agent list` - List all DAGs
- `/agent show [dag_id]` - Show DAG details
- `/agent edit [dag_id]` - Open visual DAG editor
- `/agent help` - Show command help

## Flutter Implementation

### 1. Update AgentService for WebSocket Handling

```dart
// lib/services/agent_service.dart
import 'dart:async';
import 'dart:convert';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:web_socket_channel/status.dart' as status;

class AgentService {
  static const String _baseUrl = 'ws://localhost:8080'; // or your aider-worker URL
  WebSocketChannel? _channel;
  StreamController<Map<String, dynamic>>? _messageController;

  // Stream for receiving messages from WebSocket
  Stream<Map<String, dynamic>> get messageStream =>
      _messageController?.stream ?? const Stream.empty();

  /// Send an agent command through WebSocket
  Future<void> sendAgentCommand({
    required String command,
    required String projectId,
    required String jwtToken,
  }) async {
    await _connectWebSocket(projectId, command, jwtToken);
  }

  /// Connect to WebSocket for streaming responses
  Future<void> _connectWebSocket(
    String projectId,
    String prompt,
    String jwtToken,
  ) async {
    try {
      // Close existing connection
      await _closeWebSocket();

      // Create new connection
      final uri = Uri.parse(
        '$_baseUrl/edit/stream?project_id=$projectId&prompt=${Uri.encodeComponent(prompt)}&token=$jwtToken'
      );

      _channel = WebSocketChannel.connect(uri);
      _messageController = StreamController<Map<String, dynamic>>.broadcast();

      // Listen to messages
      _channel!.stream.listen(
        (message) {
          try {
            final data = jsonDecode(message);
            _messageController?.add(data);
          } catch (e) {
            print('Error parsing WebSocket message: $e');
          }
        },
        onError: (error) {
          print('WebSocket error: $error');
          _messageController?.addError(error);
        },
        onDone: () {
          print('WebSocket connection closed');
          _messageController?.close();
        },
      );

    } catch (e) {
      print('Failed to connect WebSocket: $e');
      throw Exception('Failed to connect to agent service: $e');
    }
  }

  /// Close WebSocket connection
  Future<void> _closeWebSocket() async {
    await _channel?.sink.close(status.goingAway);
    await _messageController?.close();
    _channel = null;
    _messageController = null;
  }

  /// Check if command is an agent command
  bool isAgentCommand(String message) {
    return message.trim().startsWith('/agent ');
  }

  /// Parse agent command for UI hints
  Map<String, String> parseAgentCommand(String message) {
    final parts = message.trim().substring(7).split(' '); // Remove '/agent '

    if (parts.isEmpty) return {'command': '', 'target': '', 'description': ''};

    final command = parts[0];

    if (command == 'create') {
      return {
        'command': 'create',
        'target': '',
        'description': parts.skip(1).join(' ')
      };
    } else if (parts.length > 1) {
      return {
        'command': command,
        'target': parts[1],
        'description': parts.skip(2).join(' ')
      };
    } else {
      return {
        'command': command,
        'target': '',
        'description': ''
      };
    }
  }

  void dispose() {
    _closeWebSocket();
  }
}
```

### 2. Enhanced Chat Interface

```dart
// lib/widgets/chat_interface.dart
import 'package:flutter/material.dart';
import '../services/agent_service.dart';
import '../services/supabase_service.dart';

class ChatInterface extends StatefulWidget {
  final String projectId;

  const ChatInterface({Key? key, required this.projectId}) : super(key: key);

  @override
  State<ChatInterface> createState() => _ChatInterfaceState();
}

class _ChatInterfaceState extends State<ChatInterface> {
  final TextEditingController _messageController = TextEditingController();
  final ScrollController _scrollController = ScrollController();
  final AgentService _agentService = AgentService();
  final List<ChatMessage> _messages = [];
  bool _isProcessing = false;

  @override
  void initState() {
    super.initState();
    _setupWebSocketListener();
  }

  void _setupWebSocketListener() {
    _agentService.messageStream.listen((data) {
      setState(() {
        _handleWebSocketMessage(data);
      });
    });
  }

  void _handleWebSocketMessage(Map<String, dynamic> data) {
    final type = data['type'] as String?;
    final message = data['message'] as String?;

    switch (type) {
      case 'agent_response':
        _addMessage(ChatMessage(
          content: message ?? 'Agent processing...',
          isUser: false,
          messageType: ChatMessageType.agent,
          timestamp: DateTime.now(),
        ));
        break;

      case 'progress':
        _updateLastMessage(message ?? 'Processing...');
        break;

      case 'dag_created':
        _addMessage(ChatMessage(
          content: message ?? 'DAG created successfully!',
          isUser: false,
          messageType: ChatMessageType.success,
          timestamp: DateTime.now(),
          metadata: data,
        ));
        _isProcessing = false;
        break;

      case 'dag_list':
        _addMessage(ChatMessage(
          content: message ?? 'DAG list retrieved',
          isUser: false,
          messageType: ChatMessageType.list,
          timestamp: DateTime.now(),
          metadata: {'dags': data['dags']},
        ));
        _isProcessing = false;
        break;

      case 'dag_details':
        _addMessage(ChatMessage(
          content: message ?? 'DAG details',
          isUser: false,
          messageType: ChatMessageType.details,
          timestamp: DateTime.now(),
          metadata: {'dag': data['dag']},
        ));
        _isProcessing = false;
        break;

      case 'open_canvas':
        _openDagCanvas(data);
        _isProcessing = false;
        break;

      case 'error':
        _addMessage(ChatMessage(
          content: message ?? 'An error occurred',
          isUser: false,
          messageType: ChatMessageType.error,
          timestamp: DateTime.now(),
        ));
        _isProcessing = false;
        break;

      case 'token':
        // Regular aider streaming
        _appendToLastMessage(data['text'] as String? ?? '');
        break;

      default:
        print('Unknown message type: $type');
    }

    _scrollToBottom();
  }

  void _addMessage(ChatMessage message) {
    setState(() {
      _messages.add(message);
    });
  }

  void _updateLastMessage(String content) {
    if (_messages.isNotEmpty) {
      setState(() {
        _messages.last = _messages.last.copyWith(content: content);
      });
    }
  }

  void _appendToLastMessage(String text) {
    if (_messages.isNotEmpty) {
      setState(() {
        _messages.last = _messages.last.copyWith(
          content: _messages.last.content + text
        );
      });
    }
  }

  void _scrollToBottom() {
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (_scrollController.hasClients) {
        _scrollController.animateTo(
          _scrollController.position.maxScrollExtent,
          duration: const Duration(milliseconds: 300),
          curve: Curves.easeOut,
        );
      }
    });
  }

  Future<void> _sendMessage() async {
    final message = _messageController.text.trim();
    if (message.isEmpty || _isProcessing) return;

    // Add user message
    _addMessage(ChatMessage(
      content: message,
      isUser: true,
      messageType: ChatMessageType.user,
      timestamp: DateTime.now(),
    ));

    _messageController.clear();
    setState(() {
      _isProcessing = true;
    });

    try {
      final jwtToken = SupabaseService.instance.currentSession?.accessToken;
      if (jwtToken == null) {
        throw Exception('Not authenticated');
      }

      await _agentService.sendAgentCommand(
        command: message,
        projectId: widget.projectId,
        jwtToken: jwtToken,
      );

    } catch (e) {
      _addMessage(ChatMessage(
        content: 'Error: $e',
        isUser: false,
        messageType: ChatMessageType.error,
        timestamp: DateTime.now(),
      ));
      setState(() {
        _isProcessing = false;
      });
    }
  }

  void _openDagCanvas(Map<String, dynamic> data) {
    final dag = data['dag'] as Map<String, dynamic>?;
    if (dag != null) {
      Navigator.of(context).push(
        MaterialPageRoute(
          builder: (context) => DagCanvasView(
            dag: dag,
            projectId: widget.projectId,
            instructions: data['instructions'] as String? ?? '',
          ),
        ),
      );
    }
  }

  Widget _buildMessageInput() {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Theme.of(context).cardColor,
        border: Border(
          top: BorderSide(
            color: Theme.of(context).dividerColor,
            width: 1,
          ),
        ),
      ),
      child: Row(
        children: [
          Expanded(
            child: TextField(
              controller: _messageController,
              decoration: InputDecoration(
                hintText: _getInputHint(),
                border: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(24),
                ),
                contentPadding: const EdgeInsets.symmetric(
                  horizontal: 16,
                  vertical: 12,
                ),
                suffixIcon: _buildQuickActions(),
              ),
              maxLines: null,
              textCapitalization: TextCapitalization.sentences,
              onSubmitted: (_) => _sendMessage(),
            ),
          ),
          const SizedBox(width: 8),
          FloatingActionButton(
            onPressed: _isProcessing ? null : _sendMessage,
            mini: true,
            child: _isProcessing
              ? const SizedBox(
                  width: 16,
                  height: 16,
                  child: CircularProgressIndicator(strokeWidth: 2),
                )
              : const Icon(Icons.send),
          ),
        ],
      ),
    );
  }

  String _getInputHint() {
    if (_messages.isEmpty) {
      return 'Type /agent help for commands, or describe what you want to create...';
    }
    return 'Message agent...';
  }

  Widget? _buildQuickActions() {
    return PopupMenuButton<String>(
      icon: const Icon(Icons.add_circle_outline),
      tooltip: 'Quick Actions',
      onSelected: (value) {
        _messageController.text = value;
      },
      itemBuilder: (context) => [
        const PopupMenuItem(
          value: '/agent create daily data pipeline',
          child: Text('📊 Create Data Pipeline'),
        ),
        const PopupMenuItem(
          value: '/agent create ML training workflow',
          child: Text('🤖 Create ML Workflow'),
        ),
        const PopupMenuItem(
          value: '/agent create hourly report generator',
          child: Text('📈 Create Report Generator'),
        ),
        const PopupMenuItem(
          value: '/agent list',
          child: Text('📋 List All DAGs'),
        ),
        const PopupMenuItem(
          value: '/agent help',
          child: Text('❓ Show Help'),
        ),
      ],
    );
  }

  @override
  Widget build(BuildContext context) {
    return Column(
      children: [
        Expanded(
          child: ListView.builder(
            controller: _scrollController,
            padding: const EdgeInsets.all(16),
            itemCount: _messages.length,
            itemBuilder: (context, index) {
              return ChatMessageWidget(
                message: _messages[index],
                onDagTap: (dagId) {
                  _messageController.text = '/agent show $dagId';
                },
                onEditDag: (dagId) {
                  _messageController.text = '/agent edit $dagId';
                },
              );
            },
          ),
        ),
        _buildMessageInput(),
      ],
    );
  }

  @override
  void dispose() {
    _agentService.dispose();
    _messageController.dispose();
    _scrollController.dispose();
    super.dispose();
  }
}
```

### 3. Enhanced Chat Message Widget

```dart
// lib/widgets/chat_message_widget.dart
import 'package:flutter/material.dart';
import 'package:flutter_markdown/flutter_markdown.dart';

enum ChatMessageType {
  user,
  agent,
  success,
  error,
  list,
  details,
}

class ChatMessage {
  final String content;
  final bool isUser;
  final ChatMessageType messageType;
  final DateTime timestamp;
  final Map<String, dynamic>? metadata;

  const ChatMessage({
    required this.content,
    required this.isUser,
    required this.messageType,
    required this.timestamp,
    this.metadata,
  });

  ChatMessage copyWith({
    String? content,
    bool? isUser,
    ChatMessageType? messageType,
    DateTime? timestamp,
    Map<String, dynamic>? metadata,
  }) {
    return ChatMessage(
      content: content ?? this.content,
      isUser: isUser ?? this.isUser,
      messageType: messageType ?? this.messageType,
      timestamp: timestamp ?? this.timestamp,
      metadata: metadata ?? this.metadata,
    );
  }
}

class ChatMessageWidget extends StatelessWidget {
  final ChatMessage message;
  final Function(String)? onDagTap;
  final Function(String)? onEditDag;

  const ChatMessageWidget({
    Key? key,
    required this.message,
    this.onDagTap,
    this.onEditDag,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.only(bottom: 16),
      child: Row(
        mainAxisAlignment: message.isUser
          ? MainAxisAlignment.end
          : MainAxisAlignment.start,
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          if (!message.isUser) _buildAvatar(context),
          Flexible(
            child: Container(
              constraints: BoxConstraints(
                maxWidth: MediaQuery.of(context).size.width * 0.8,
              ),
              padding: const EdgeInsets.all(12),
              decoration: BoxDecoration(
                color: _getBackgroundColor(context),
                borderRadius: BorderRadius.circular(16),
                border: message.messageType == ChatMessageType.error
                  ? Border.all(color: Colors.red.shade300)
                  : null,
              ),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  _buildContent(context),
                  if (message.messageType == ChatMessageType.list)
                    _buildDagList(context),
                  if (message.messageType == ChatMessageType.details)
                    _buildDagDetails(context),
                  const SizedBox(height: 4),
                  _buildTimestamp(context),
                ],
              ),
            ),
          ),
          if (message.isUser) _buildAvatar(context),
        ],
      ),
    );
  }

  Widget _buildAvatar(BuildContext context) {
    return Container(
      margin: EdgeInsets.only(
        left: message.isUser ? 8 : 0,
        right: message.isUser ? 0 : 8,
      ),
      child: CircleAvatar(
        radius: 16,
        backgroundColor: message.isUser
          ? Theme.of(context).primaryColor
          : Colors.grey.shade600,
        child: Icon(
          message.isUser ? Icons.person : Icons.smart_toy,
          size: 16,
          color: Colors.white,
        ),
      ),
    );
  }

  Color _getBackgroundColor(BuildContext context) {
    if (message.isUser) {
      return Theme.of(context).primaryColor.withOpacity(0.1);
    }

    switch (message.messageType) {
      case ChatMessageType.success:
        return Colors.green.shade50;
      case ChatMessageType.error:
        return Colors.red.shade50;
      case ChatMessageType.list:
        return Colors.blue.shade50;
      case ChatMessageType.details:
        return Colors.purple.shade50;
      default:
        return Theme.of(context).cardColor;
    }
  }

  Widget _buildContent(BuildContext context) {
    return MarkdownBody(
      data: message.content,
      styleSheet: MarkdownStyleSheet(
        p: TextStyle(
          color: Theme.of(context).textTheme.bodyLarge?.color,
          fontSize: 14,
        ),
        code: TextStyle(
          backgroundColor: Colors.grey.shade200,
          fontFamily: 'monospace',
        ),
      ),
    );
  }

  Widget _buildDagList(BuildContext context) {
    final dags = message.metadata?['dags'] as List<dynamic>?;
    if (dags == null || dags.isEmpty) return const SizedBox.shrink();

    return Column(
      children: [
        const SizedBox(height: 8),
        ...dags.map((dag) => _buildDagListItem(context, dag)).toList(),
      ],
    );
  }

  Widget _buildDagListItem(BuildContext context, dynamic dag) {
    final dagMap = dag as Map<String, dynamic>;
    final dagId = dagMap['dag_id'] as String;
    final dagName = dagMap['dag_name'] as String;
    final description = dagMap['description'] as String;
    final taskCount = dagMap['task_count'] as int;

    return Card(
      margin: const EdgeInsets.only(bottom: 8),
      child: ListTile(
        leading: const Icon(Icons.account_tree),
        title: Text(dagName),
        subtitle: Text('$description\n$taskCount tasks'),
        isThreeLine: true,
        trailing: PopupMenuButton<String>(
          onSelected: (action) {
            if (action == 'show' && onDagTap != null) {
              onDagTap!(dagId);
            } else if (action == 'edit' && onEditDag != null) {
              onEditDag!(dagId);
            }
          },
          itemBuilder: (context) => [
            const PopupMenuItem(
              value: 'show',
              child: Text('👁️ View Details'),
            ),
            const PopupMenuItem(
              value: 'edit',
              child: Text('✏️ Edit'),
            ),
          ],
        ),
        onTap: () => onDagTap?.call(dagId),
      ),
    );
  }

  Widget _buildDagDetails(BuildContext context) {
    final dag = message.metadata?['dag'] as Map<String, dynamic>?;
    if (dag == null) return const SizedBox.shrink();

    return Card(
      margin: const EdgeInsets.only(top: 8),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Row(
              children: [
                const Icon(Icons.account_tree, size: 20),
                const SizedBox(width: 8),
                Expanded(
                  child: Text(
                    dag['dag_name'] as String,
                    style: const TextStyle(fontWeight: FontWeight.bold),
                  ),
                ),
                IconButton(
                  icon: const Icon(Icons.edit),
                  onPressed: () => onEditDag?.call(dag['dag_id'] as String),
                  tooltip: 'Edit DAG',
                ),
              ],
            ),
            const SizedBox(height: 8),
            _buildDetailRow('Schedule', dag['schedule_interval'] as String),
            _buildDetailRow('Tasks', '${dag['task_count']} tasks'),
            _buildDetailRow('Owner', dag['owner'] as String),
            if (dag['tags'] != null && (dag['tags'] as List).isNotEmpty)
              _buildDetailRow('Tags', (dag['tags'] as List).join(', ')),
          ],
        ),
      ),
    );
  }

  Widget _buildDetailRow(String label, String value) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 4),
      child: Row(
        children: [
          SizedBox(
            width: 80,
            child: Text(
              '$label:',
              style: const TextStyle(fontWeight: FontWeight.w500),
            ),
          ),
          Expanded(
            child: Text(value),
          ),
        ],
      ),
    );
  }

  Widget _buildTimestamp(BuildContext context) {
    return Text(
      '${message.timestamp.hour.toString().padLeft(2, '0')}:${message.timestamp.minute.toString().padLeft(2, '0')}',
      style: TextStyle(
        fontSize: 10,
        color: Theme.of(context).textTheme.bodySmall?.color,
      ),
    );
  }
}
```

### 4. DAG Canvas View (Placeholder)

```dart
// lib/views/dag_canvas_view.dart
import 'package:flutter/material.dart';

class DagCanvasView extends StatefulWidget {
  final Map<String, dynamic> dag;
  final String projectId;
  final String instructions;

  const DagCanvasView({
    Key? key,
    required this.dag,
    required this.projectId,
    required this.instructions,
  }) : super(key: key);

  @override
  State<DagCanvasView> createState() => _DagCanvasViewState();
}

class _DagCanvasViewState extends State<DagCanvasView> {
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: Text('Edit ${widget.dag['dag_name']}'),
        actions: [
          IconButton(
            icon: const Icon(Icons.save),
            onPressed: () {
              // TODO: Implement save functionality
              Navigator.pop(context);
            },
          ),
        ],
      ),
      body: Center(
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            const Icon(
              Icons.account_tree,
              size: 64,
              color: Colors.grey,
            ),
            const SizedBox(height: 16),
            Text(
              'Visual DAG Editor',
              style: Theme.of(context).textTheme.headlineSmall,
            ),
            const SizedBox(height: 8),
            Text(
              'Canvas interface for ${widget.dag['dag_name']}',
              style: Theme.of(context).textTheme.bodyMedium,
            ),
            const SizedBox(height: 16),
            if (widget.instructions.isNotEmpty)
              Card(
                child: Padding(
                  padding: const EdgeInsets.all(16),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text(
                        'Instructions:',
                        style: TextStyle(fontWeight: FontWeight.bold),
                      ),
                      const SizedBox(height: 8),
                      Text(widget.instructions),
                    ],
                  ),
                ),
              ),
            const SizedBox(height: 16),
            ElevatedButton(
              onPressed: () {
                Navigator.pop(context);
              },
              child: const Text('Close Editor'),
            ),
          ],
        ),
      ),
    );
  }
}
```

## Usage Examples

### 1. Creating DAGs with Natural Language

```dart
// Users can type in the chat:
"/agent create daily data pipeline that extracts user analytics from database, transforms the data, and sends reports to stakeholders"

"/agent create ML model training workflow that runs weekly on user behavior data"

"/agent create hourly system health check that monitors server status and sends alerts if issues detected"
```

### 2. Managing Existing DAGs

```dart
// List all DAGs
"/agent list"

// Show specific DAG details
"/agent show my_data_pipeline"

// Edit DAG in visual canvas
"/agent edit user_analytics_dag"

// Get help
"/agent help"
```

### 3. Integration with Existing Chat

The agent commands seamlessly integrate with your existing chat interface. Users can:

1. **Type natural language descriptions** to create DAGs
2. **Get real-time progress updates** during DAG creation
3. **View interactive DAG lists** with action buttons
4. **Open the visual canvas editor** for complex edits
5. **Mix agent commands with regular chat** for explanations

## WebSocket Message Types

The implementation sends these message types through WebSocket:

- `agent_response` - Initial command acknowledgment
- `progress` - Real-time progress updates during DAG creation
- `dag_created` - Successful DAG creation with details
- `dag_list` - List of DAGs with interactive elements
- `dag_details` - Detailed information about a specific DAG
- `open_canvas` - Signal to open visual DAG editor
- `error` - Error messages with helpful context

## Benefits

✅ **Natural Language Interface** - Users describe what they want instead of writing code  
✅ **Real-time Feedback** - Progress updates during DAG creation  
✅ **Seamless Integration** - Works within existing chat interface  
✅ **Visual Editing** - Canvas opens for complex modifications  
✅ **Smart Suggestions** - Quick action buttons for common tasks  
✅ **Error Handling** - Clear error messages with suggestions

## Next Steps

1. **Implement LLM Integration** for better natural language understanding
2. **Build Visual Canvas** for drag-and-drop DAG editing
3. **Add DAG Templates** for common use cases
4. **Enhance Validation** with real-time syntax checking
5. **Add Collaboration** features for team DAG editing

This integration provides a powerful, user-friendly way to create and manage DAGs through natural language commands in your Flutter app! 🚀


