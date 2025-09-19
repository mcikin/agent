-- Create table for tracking user-created DAGs
-- Run this SQL in your Supabase SQL Editor

CREATE TABLE IF NOT EXISTS user_dags (
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
    tasks JSONB,  -- Store complete task specifications for Flutter app visualization
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    commit_id TEXT,
    
    -- Ensure unique DAG ID per project
    UNIQUE(project_id, dag_id)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_user_dags_project ON user_dags(project_id);
CREATE INDEX IF NOT EXISTS idx_user_dags_user ON user_dags(user_id);
CREATE INDEX IF NOT EXISTS idx_user_dags_dag_id ON user_dags(dag_id);
CREATE INDEX IF NOT EXISTS idx_user_dags_created_at ON user_dags(created_at DESC);

-- Enable RLS (Row Level Security)
ALTER TABLE user_dags ENABLE ROW LEVEL SECURITY;

-- Create policy for service role (aider-worker service)
CREATE POLICY "Service role can manage user_dags" ON user_dags
FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Optional: Create policy for authenticated users to read their own DAGs
CREATE POLICY "Users can read their own DAGs" ON user_dags
FOR SELECT TO authenticated
USING (user_id = auth.uid());

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_user_dags_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_user_dags_updated_at_trigger
    BEFORE UPDATE ON user_dags
    FOR EACH ROW
    EXECUTE FUNCTION update_user_dags_updated_at();

-- Migration: Add tasks column for Flutter app visualization
-- Run this if you have existing user_dags table without tasks column
ALTER TABLE user_dags ADD COLUMN IF NOT EXISTS tasks JSONB;

-- Add comments for documentation
COMMENT ON TABLE user_dags IS 'Tracks DAGs created by users through the Flutter app';
COMMENT ON COLUMN user_dags.project_id IS 'UUID of the project this DAG belongs to';
COMMENT ON COLUMN user_dags.user_id IS 'UUID of the user who created this DAG';
COMMENT ON COLUMN user_dags.dag_id IS 'Unique identifier for the DAG within Airflow';
COMMENT ON COLUMN user_dags.dag_name IS 'Human-readable name of the DAG';
COMMENT ON COLUMN user_dags.file_path IS 'Path to the DAG file relative to project root';
COMMENT ON COLUMN user_dags.schedule_interval IS 'Cron expression or Airflow preset for scheduling';
COMMENT ON COLUMN user_dags.task_count IS 'Number of tasks in this DAG';
COMMENT ON COLUMN user_dags.tasks IS 'Complete task specifications for Flutter app visualization';
COMMENT ON COLUMN user_dags.is_active IS 'Whether the DAG is currently active/enabled';
COMMENT ON COLUMN user_dags.commit_id IS 'Git commit ID when this DAG was last modified';


