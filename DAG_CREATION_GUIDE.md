# DAG Creation Implementation Guide

## Overview

This implementation adds DAG creation functionality to the existing aider-worker service, allowing users to create custom Airflow DAGs through their Flutter app with full Supabase integration.

## Features

✅ **User-driven DAG creation** from custom input  
✅ **Template-based generation** with Python/Bash tasks  
✅ **Supabase sync** for both files and metadata  
✅ **Git integration** with automatic commits  
✅ **User permission verification** via JWT  
✅ **Database tracking** of all user-created DAGs  
✅ **RESTful API** endpoints for DAG management

## Architecture

```
Flutter App → POST /create-dag → DAG Generator → File System → Git Commit → Supabase Storage + DB
```

## Database Schema

The implementation adds a new `user_dags` table:

```sql
-- Run this in your Supabase SQL Editor
CREATE TABLE user_dags (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    project_id UUID NOT NULL,
    user_id UUID NOT NULL,
    dag_id TEXT NOT NULL,
    dag_name TEXT NOT NULL,
    description TEXT,
    file_path TEXT NOT NULL,
    schedule_interval TEXT,
    owner TEXT,
    tags TEXT[],
    task_count INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    commit_id TEXT,
    UNIQUE(project_id, dag_id)
);
```

## API Endpoints

### 1. Create DAG

```bash
POST /create-dag
```

Creates a new DAG from user input and syncs with Supabase.

**Request Body:**

```json
{
  "project_id": "123e4567-e89b-12d3-a456-426614174000",
  "dag_name": "My Custom ETL Pipeline",
  "description": "Processes daily user data",
  "schedule_interval": "@daily",
  "owner": "data-team",
  "tags": ["etl", "production", "daily"],
  "start_date": "2024-01-01T00:00:00Z",
  "retries": 2,
  "retry_delay_minutes": 5,
  "depends_on_past": false,
  "catchup": false,
  "email_on_failure": false,
  "email_on_retry": false,
  "tasks": [
    {
      "task_id": "extract_data",
      "task_type": "python",
      "python_code": "import pandas as pd\nprint('Extracting data...')\ndata = pd.read_csv('/data/input.csv')\nreturn data.to_dict()",
      "provide_context": false,
      "dependencies": []
    },
    {
      "task_id": "transform_data",
      "task_type": "python",
      "python_code": "def transform_data(**context):\n    ti = context['ti']\n    data = ti.xcom_pull(task_ids='extract_data')\n    # Transform data here\n    return data",
      "provide_context": true,
      "dependencies": ["extract_data"]
    },
    {
      "task_id": "load_to_db",
      "task_type": "bash",
      "bash_command": "echo 'Loading data to database...' && python /scripts/load_data.py",
      "dependencies": ["transform_data"]
    }
  ]
}
```

**Response:**

```json
{
  "dag_id": "my_custom_etl_pipeline",
  "file_path": "dags/my_custom_etl_pipeline.py",
  "commit_id": "abc123...",
  "created_at": "2024-01-15T10:30:00Z"
}
```

### 2. List DAGs

```bash
GET /dags?project_id={project_id}&user_id={user_id}
```

**Response:**

```json
{
  "dags": [
    {
      "id": "uuid-here",
      "project_id": "123e4567-e89b-12d3-a456-426614174000",
      "user_id": "user-uuid",
      "dag_id": "my_custom_etl_pipeline",
      "dag_name": "My Custom ETL Pipeline",
      "description": "Processes daily user data",
      "file_path": "dags/my_custom_etl_pipeline.py",
      "schedule_interval": "@daily",
      "owner": "data-team",
      "tags": ["etl", "production", "daily"],
      "task_count": 3,
      "is_active": true,
      "created_at": "2024-01-15T10:30:00Z",
      "updated_at": "2024-01-15T10:30:00Z",
      "commit_id": "abc123..."
    }
  ]
}
```

### 3. Get DAG Details

```bash
GET /dags/{dag_id}?project_id={project_id}
```

Returns detailed information about a specific DAG.

## Usage Examples

### Example 1: Simple Data Processing DAG

```bash
curl -X POST http://localhost:8080/create-dag \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "123e4567-e89b-12d3-a456-426614174000",
    "dag_name": "Daily Report Generator",
    "description": "Generates daily reports from user analytics",
    "schedule_interval": "@daily",
    "owner": "analytics-team",
    "tags": ["reporting", "analytics"],
    "tasks": [
        {
            "task_id": "fetch_analytics",
            "task_type": "python",
            "python_code": "print(\"Fetching analytics data...\")\nreturn {\"users\": 1000, \"sessions\": 5000}"
        },
        {
            "task_id": "generate_report",
            "task_type": "bash",
            "bash_command": "echo \"Generating report...\" && python /scripts/generate_report.py",
            "dependencies": ["fetch_analytics"]
        }
    ]
}'
```

### Example 2: Machine Learning Pipeline

```bash
curl -X POST http://localhost:8080/create-dag \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "123e4567-e89b-12d3-a456-426614174000",
    "dag_name": "ML Model Training",
    "description": "Trains and deploys ML models",
    "schedule_interval": "0 2 * * *",
    "owner": "ml-team",
    "tags": ["ml", "training"],
    "retries": 3,
    "retry_delay_minutes": 10,
    "tasks": [
        {
            "task_id": "prepare_data",
            "task_type": "python",
            "python_code": "import numpy as np\nprint(\"Preparing training data...\")\ndata = np.random.rand(1000, 10)\nreturn data.shape"
        },
        {
            "task_id": "train_model",
            "task_type": "python",
            "python_code": "print(\"Training model...\")\naccuracy = 0.95\nreturn {\"accuracy\": accuracy}",
            "dependencies": ["prepare_data"]
        },
        {
            "task_id": "deploy_model",
            "task_type": "bash",
            "bash_command": "echo \"Deploying model to production...\" && kubectl apply -f model-deployment.yaml",
            "dependencies": ["train_model"]
        }
    ]
}'
```

## Generated DAG Structure

The system generates Airflow DAGs with this structure:

```python
# Generated DAG: My Custom ETL Pipeline
# Created by: user-uuid
# Generated at: 2024-01-15T10:30:00Z

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Python functions for tasks
def extract_data_function():
    import pandas as pd
    print('Extracting data...')
    data = pd.read_csv('/data/input.csv')
    return data.to_dict()

def transform_data_function(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    # Transform data here
    return data

# DAG default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'my_custom_etl_pipeline',
    default_args=default_args,
    description='Processes daily user data',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'production', 'daily'],
)

# Task definitions
extract_data_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_function,
    provide_context=False,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_function,
    provide_context=True,
    dag=dag,
)

load_to_db_task = BashOperator(
    task_id='load_to_db',
    bash_command='''echo 'Loading data to database...' && python /scripts/load_data.py''',
    dag=dag,
)

# Task dependencies
extract_data_task >> transform_data_task
transform_data_task >> load_to_db_task
```

## Integration Flow

1. **User Input**: Flutter app collects DAG requirements from user
2. **Authentication**: JWT token verified via Supabase
3. **Permission Check**: User membership in project validated
4. **DAG Generation**: Template engine creates Airflow DAG file
5. **File Creation**: DAG file written to project directory
6. **Git Commit**: Changes committed to local repository
7. **Supabase Upload**: File uploaded to Supabase storage
8. **Database Record**: DAG metadata stored in `user_dags` table
9. **Response**: DAG details returned to Flutter app

## Flutter Integration

Add these methods to your `AgentService`:

```dart
Future<Map<String, dynamic>> createDAG({
  required String projectId,
  required String dagName,
  required String description,
  required String scheduleInterval,
  required String owner,
  required List<Map<String, dynamic>> tasks,
  List<String>? tags,
}) async {
  final response = await _httpClient.post(
    Uri.parse('${_baseUrl}/create-dag'),
    headers: {
      'Authorization': 'Bearer ${_supabaseClient.auth.currentSession?.accessToken}',
      'Content-Type': 'application/json',
    },
    body: jsonEncode({
      'project_id': projectId,
      'dag_name': dagName,
      'description': description,
      'schedule_interval': scheduleInterval,
      'owner': owner,
      'tasks': tasks,
      'tags': tags ?? [],
    }),
  );

  if (response.statusCode == 200) {
    return jsonDecode(response.body);
  } else {
    throw Exception('Failed to create DAG: ${response.body}');
  }
}

Future<List<Map<String, dynamic>>> listDAGs(String projectId) async {
  final response = await _httpClient.get(
    Uri.parse('${_baseUrl}/dags?project_id=$projectId'),
    headers: {
      'Authorization': 'Bearer ${_supabaseClient.auth.currentSession?.accessToken}',
    },
  );

  if (response.statusCode == 200) {
    final data = jsonDecode(response.body);
    return List<Map<String, dynamic>>.from(data['dags']);
  } else {
    throw Exception('Failed to list DAGs: ${response.body}');
  }
}
```

## Error Handling

The implementation includes comprehensive error handling:

- **Validation**: All input is validated before processing
- **Rollback**: Failed operations clean up created files
- **Logging**: All operations are logged for debugging
- **HTTP Errors**: Proper status codes and error messages
- **Database Constraints**: Unique DAG IDs enforced per project

## Security

- **JWT Authentication**: All endpoints require valid JWT tokens
- **Project Membership**: Users can only create DAGs in projects they belong to
- **Input Sanitization**: DAG names and code are sanitized
- **Row Level Security**: Database access controlled via Supabase RLS

## Monitoring

Track DAG creation and usage through:

1. **Database Queries**: Monitor `user_dags` table
2. **Supabase Logs**: Check storage and database operations
3. **Application Logs**: Monitor aider-worker service logs
4. **Git History**: Track DAG file changes in repositories

## Future Enhancements

Potential improvements:

- **DAG Templates**: Pre-built templates for common patterns
- **Visual DAG Builder**: Drag-and-drop interface in Flutter
- **DAG Validation**: Syntax checking before creation
- **DAG Scheduling**: Integration with Airflow scheduler
- **Version Control**: Track DAG versions and rollback capability
- **Sharing**: Allow users to share DAG templates
- **Import/Export**: Backup and restore DAG configurations

## Troubleshooting

### Common Issues

1. **"DAG name already exists"**

   - Solution: The system auto-appends numbers to duplicate names

2. **"Invalid schedule interval"**

   - Solution: Use valid cron expressions or Airflow presets (@daily, @hourly, etc.)

3. **"Permission denied"**

   - Solution: Ensure user is a member of the project

4. **"Failed to upload to Supabase"**
   - Solution: Check Supabase credentials and bucket permissions

### Database Migration

To set up the database table, run the SQL from `/aider-worker/db_migrations/create_dag_table.sql` in your Supabase SQL Editor.

## Testing

Test the complete flow:

1. Set up a test project with proper permissions
2. Create a simple DAG using the API
3. Verify file creation in git repository
4. Check Supabase storage for uploaded file
5. Confirm database record in `user_dags` table
6. Test DAG listing and retrieval endpoints
