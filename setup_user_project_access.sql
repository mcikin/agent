-- ===========================================
-- USER-PROJECT ACCESS SETUP
-- ===========================================
-- This script sets up automatic user-project association
-- so users don't need manual project membership setup

-- 1. Define the default project ID (consistent across all users)
-- This project ID will be used by ALL users in your Flutter app
-- const String DEFAULT_PROJECT_ID = '550e8400-e29b-41d4-a716-446655440000';

-- 2. Add yourself to the default project
INSERT INTO project_members (project_id, user_id, role)
VALUES (
    '550e8400-e29b-41d4-a716-446655440000',
    'b5ee17fd-d23d-4893-b966-e804118582f9',
    'owner'
) ON CONFLICT (project_id, user_id) DO NOTHING;

-- 3. Create a function to automatically add users to default project
CREATE OR REPLACE FUNCTION add_user_to_default_project()
RETURNS TRIGGER AS $$
BEGIN
    -- Add new user to default project
    INSERT INTO project_members (project_id, user_id, role)
    VALUES (
        '550e8400-e29b-41d4-a716-446655440000',
        NEW.id,
        'member'
    )
    ON CONFLICT (project_id, user_id) DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 4. Create trigger for automatic user addition
-- This will automatically add new users to the default project
DROP TRIGGER IF EXISTS add_user_to_default_project_trigger ON auth.users;
CREATE TRIGGER add_user_to_default_project_trigger
    AFTER INSERT ON auth.users
    FOR EACH ROW
    EXECUTE FUNCTION add_user_to_default_project();

-- 5. Alternative: Function to add existing users to default project
CREATE OR REPLACE FUNCTION add_all_users_to_default_project()
RETURNS void AS $$
BEGIN
    -- Add all existing users to default project
    INSERT INTO project_members (project_id, user_id, role)
    SELECT
        '550e8400-e29b-41d4-a716-446655440000',
        id,
        'member'
    FROM auth.users
    ON CONFLICT (project_id, user_id) DO NOTHING;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 6. Run the function to add existing users
SELECT add_all_users_to_default_project();

-- 7. Verify setup
SELECT
    pm.project_id,
    pm.user_id,
    pm.role,
    u.email
FROM project_members pm
JOIN auth.users u ON pm.user_id = u.id
WHERE pm.project_id = '550e8400-e29b-41d4-a716-446655440000';

-- ===========================================
-- USAGE IN FLUTTER APP
-- ===========================================

-- Always use this consistent project ID in your Flutter app:
const String DEFAULT_PROJECT_ID = '550e8400-e29b-41d4-a716-446655440000';

-- This project will be accessible to all authenticated users automatically

-- ===========================================
-- ALTERNATIVE: SIMPLE APPROACH (No Database Triggers)
-- ===========================================

-- If you prefer not to use database triggers, here's a simpler approach:

-- 1. Just use the default project ID in your Flutter app
-- 2. Manually add users to this project when they sign up
-- 3. Or add them when they first try to use the app

-- Flutter App Code Example:
/*
Future<void> ensureUserInDefaultProject() async {
  final user = Supabase.instance.client.auth.currentUser;
  if (user == null) return;

  try {
    // Try to access the default project
    final response = await http.get(
      Uri.parse('http://localhost:8080/dags?project_id=$DEFAULT_PROJECT_ID'),
      headers: {
        'Authorization': 'Bearer ${user.session?.accessToken}',
      },
    );

    if (response.statusCode == 403) {
      // User is not a member, show message to contact admin
      // Or automatically add them if you have admin privileges
    }
  } catch (e) {
    print('Error checking project membership: $e');
  }
}
*/

-- ===========================================
-- MANUAL USER ADDITION (for existing users)
-- ===========================================

-- To manually add a user to the default project:
/*
INSERT INTO project_members (project_id, user_id, role)
VALUES (
    '550e8400-e29b-41d4-a716-446655440000',
    'USER_ID_HERE',
    'member'
) ON CONFLICT (project_id, user_id) DO NOTHING;
*/

-- This approach is simpler and doesn't require database triggers
