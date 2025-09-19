"""
Agent command processor for handling /agent commands in chat interface.
This bridges the chat interface with DAG creation and management functionality.
"""

import json
import logging
import re
import time
from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID
from datetime import datetime

from ..models.dto import CreateDagReq
from .dag_prompt_templates import DagPromptTemplates, DagValidator
from .code_quality_checker import CodeQualityChecker
from .logging_config import get_logger
from .metrics import track_llm_operation

# Initialize logger
logger = get_logger(__name__)


class AgentCommandProcessor:
    """Processes /agent commands from chat interface."""
    
    def __init__(self):
        self.commands = {
            'create': self._handle_create,
            'list': self._handle_list,
            'show': self._handle_show,
            'edit': self._handle_edit,
            'delete': self._handle_delete,
            'help': self._handle_help,
        }
    
    def is_agent_command(self, message: str) -> bool:
        """Check if message is an /agent command."""
        result = message.strip().startswith('/agent ')
        logger.debug(f"Checking if message is agent command: {result}")
        return result
    
    def parse_command(self, message: str) -> Tuple[str, str, str]:
        """
        Parse /agent command into components.

        Returns:
            Tuple[str, str, str]: (command, target, description)

        Examples:
            "/agent create daily data pipeline" -> ("create", "", "daily data pipeline")
            "/agent edit my_dag" -> ("edit", "my_dag", "")
            "/agent show pipeline_dag" -> ("show", "pipeline_dag", "")
        """
        print(f"AgentCommandProcessor.parse_command('{message}')")
        message = message.strip()

        # Remove /agent prefix
        if not message.startswith('/agent '):
            print("Message doesn't start with /agent")
            return "", "", ""

        parts = message[7:].strip().split(' ', 2)
        print(f"Parsed parts: {parts}")

        if len(parts) == 0:
            return "", "", ""
        elif len(parts) == 1:
            result = (parts[0], "", "")
            print(f"Parsed result: {result}")
            return result
        elif len(parts) == 2:
            # Could be "create description" or "edit dagname"
            command = parts[0]
            if command in ['create']:
                result = (command, "", parts[1])
                print(f"Parsed result: {result}")
                return result
            else:
                result = (command, parts[1], "")
                print(f"Parsed result: {result}")
                return result
        else:
            # command target description or command description
            command = parts[0]
            if command in ['create']:
                result = (command, "", " ".join(parts[1:]))
                print(f"Parsed result: {result}")
                return result
            else:
                result = (command, parts[1], " ".join(parts[2:]))
                print(f"Parsed result: {result}")
                return result
    
    async def process_command(
        self,
        message: str,
        project_id: UUID,
        user_id: str,
        llm_client=None,
        websocket=None,
        start_time=None
    ) -> Dict[str, Any]:
        """
        Process an /agent command and return structured response.

        Returns:
            Dict with keys: success, action, data, response_text
        """
        logger.info(f"Processing agent command for project {project_id}, user {user_id}")

        if not self.is_agent_command(message):
            logger.warning("Received non-agent command")
            return {
                "success": False,
                "action": "error",
                "data": {},
                "response_text": "Not an /agent command"
            }

        command, target, description = self.parse_command(message)
        logger.info(f"Parsed command: '{command}', target: '{target}'")

        if command not in self.commands:
            logger.warning(f"Unknown command: {command}")
            return {
                "success": False,
                "action": "error",
                "data": {},
                "response_text": f"Unknown command: {command}. Use /agent help for available commands."
            }

        try:
            print(f"Executing command handler for: {command}")
            result = await self.commands[command](target, description, project_id, user_id, llm_client, websocket, start_time)
            print(f"Command handler result: {result}")
            return result
        except Exception as e:
            print(f"Exception in command handler: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                "success": False,
                "action": "error",
                "data": {"error": str(e)},
                "response_text": f"Error processing command: {str(e)}"
            }
    
    async def _handle_create(
        self,
        target: str,
        description: str,
        project_id: UUID,
        user_id: str,
        llm_client=None,
        websocket=None,
        start_time=None
    ) -> Dict[str, Any]:
        """Handle /agent create command."""

        if not description.strip():
            return {
                "success": False,
                "action": "error",
                "data": {},
                "response_text": "Please provide a description for the DAG. Example: '/agent create daily data pipeline that processes user analytics'"
            }

        # Determine complexity level from description
        complexity = self._determine_complexity(description)

        # Use LLM to convert natural language to DAG specification with structured prompts
        dag_spec = await self._convert_description_to_dag_spec(description, llm_client, complexity, websocket, start_time)

        if not dag_spec:
            return {
                "success": False,
                "action": "error",
                "data": {},
                "response_text": "Could not understand the DAG description. Please be more specific about what you want the DAG to do."
            }

        # Comprehensive validation of the DAG specification
        validation_result = DagValidator.validate_dag_spec(dag_spec)

        # Additional code quality analysis
        code_quality_result = CodeQualityChecker.analyze_dag_tasks(dag_spec.get('tasks', []))
        validation_result['code_quality'] = code_quality_result

        if not validation_result["is_valid"]:
            # Try to refine the DAG spec if validation fails
            if llm_client and validation_result["issues"]:
                print(f"Validation failed, attempting refinement for {len(validation_result['issues'])} issues")
                refined_spec = await self._refine_dag_spec(dag_spec, validation_result["issues"], llm_client, websocket, start_time)
                if refined_spec:
                    print("DAG spec refined successfully")
                    dag_spec = refined_spec
                    validation_result = DagValidator.validate_dag_spec(dag_spec)

            # If still not valid, show detailed warnings but allow creation
            if not validation_result["is_valid"]:
                validation_summary = validation_result.get("validation_summary", {})
                critical_issues = [i for i in validation_result["issues"] if any(word in i.lower() for word in ["required", "invalid", "missing"])]

                warning_text = f"\n\n⚠️ **Validation Issues ({len(validation_result['issues'])} total)**\n"
                if critical_issues:
                    warning_text += "**Critical Issues:**\n" + "\n".join(f"❌ {issue}" for issue in critical_issues[:3]) + "\n\n"

                warning_text += "**All Issues:**\n" + "\n".join(f"• {issue}" for issue in validation_result["issues"][:5])

                if len(validation_result["issues"]) > 5:
                    warning_text += f"\n... and {len(validation_result['issues']) - 5} more issues"

                if validation_result.get("suggestions"):
                    warning_text += "\n\n💡 **Suggestions:**\n" + "\n".join(f"• {sugg}" for sugg in validation_result["suggestions"][:3])

                return {
                    "success": True,
                    "action": "create_dag",
                    "data": {
                        "dag_spec": dag_spec,
                        "project_id": str(project_id),
                        "user_id": user_id,
                        "validation_result": validation_result
                    },
                    "response_text": f"🤖 Creating DAG: **{dag_spec['dag_name']}**\n\n" +
                                   f"📝 Description: {dag_spec['description']}\n" +
                                   f"⏰ Schedule: {dag_spec['schedule_interval']}\n" +
                                   f"🔧 Tasks: {len(dag_spec['tasks'])}\n" +
                                   f"📊 Complexity: {dag_spec.get('complexity', 'medium')}\n" +
                                   f"✅ Validation Score: {validation_summary.get('passed_checks', 0)}/{validation_summary.get('total_checks', 6)}\n" +
                                   warning_text + "\n\n" +
                                   "⚠️ Proceeding with validation warnings - DAG will be created but may need manual review..."
                }

        # Validation passed successfully
        validation_summary = validation_result.get("validation_summary", {})
        warnings = validation_result.get("warnings", [])

        success_text = f"🤖 Creating DAG: **{dag_spec['dag_name']}**\n\n" + \
                      f"📝 Description: {dag_spec['description']}\n" + \
                      f"⏰ Schedule: {dag_spec['schedule_interval']}\n" + \
                      f"🔧 Tasks: {len(dag_spec['tasks'])}\n" + \
                      f"📊 Complexity: {dag_spec.get('complexity', 'medium')}\n" + \
                      f"✅ Validation Score: {validation_summary.get('passed_checks', 6)}/{validation_summary.get('total_checks', 6)}\n"

        if warnings:
            success_text += f"ℹ️ **Validation Notes:** {', '.join(warnings[:2])}\n"

        success_text += "\n🎯 **Repository Structure:**\n" + \
                       f"• Main DAG: `dags/{dag_spec['dag_id']}/{dag_spec['dag_id']}.py`\n" + \
                       f"• Config: `dags/{dag_spec['dag_id']}/config.json`\n" + \
                       f"• Utils: `dags/{dag_spec['dag_id']}/utils/`\n" + \
                       f"• Docs: `dags/{dag_spec['dag_id']}/README.md`\n\n" + \
                       "✅ All validations passed - creating production-ready DAG now..."

        return {
            "success": True,
            "action": "create_dag",
            "data": {
                "dag_spec": dag_spec,
                "project_id": str(project_id),
                "user_id": user_id,
                "validation_result": validation_result
            },
            "response_text": success_text
        }
    
    async def _handle_list(
        self, 
        target: str, 
        description: str, 
        project_id: UUID, 
        user_id: str,
        llm_client=None,
        websocket=None,
        start_time=None
    ) -> Dict[str, Any]:
        """Handle /agent list command."""
        return {
            "success": True,
            "action": "list_dags",
            "data": {
                "project_id": str(project_id),
                "user_id": user_id if target != "all" else None
            },
            "response_text": "📋 Listing your DAGs..."
        }
    
    async def _handle_show(
        self, 
        target: str, 
        description: str, 
        project_id: UUID, 
        user_id: str,
        llm_client=None,
        websocket=None,
        start_time=None
    ) -> Dict[str, Any]:
        """Handle /agent show command."""
        if not target:
            return {
                "success": False,
                "action": "error",
                "data": {},
                "response_text": "Please specify which DAG to show. Example: '/agent show my_pipeline_dag'"
            }
        
        return {
            "success": True,
            "action": "show_dag",
            "data": {
                "project_id": str(project_id),
                "dag_id": target
            },
            "response_text": f"🔍 Showing details for DAG: **{target}**"
        }
    
    async def _handle_edit(
        self, 
        target: str, 
        description: str, 
        project_id: UUID, 
        user_id: str,
        llm_client=None,
        websocket=None,
        start_time=None
    ) -> Dict[str, Any]:
        """Handle /agent edit command."""
        if not target:
            return {
                "success": False,
                "action": "error",
                "data": {},
                "response_text": "Please specify which DAG to edit. Example: '/agent edit my_pipeline_dag'"
            }
        
        return {
            "success": True,
            "action": "edit_dag",
            "data": {
                "project_id": str(project_id),
                "dag_id": target,
                "instructions": description
            },
            "response_text": f"✏️ Opening DAG **{target}** for editing...\n\n" +
                           "Opening visual canvas with DAG tasks and dependencies."
        }
    
    async def _handle_delete(
        self, 
        target: str, 
        description: str, 
        project_id: UUID, 
        user_id: str,
        llm_client=None,
        websocket=None,
        start_time=None
    ) -> Dict[str, Any]:
        """Handle /agent delete command."""
        if not target:
            return {
                "success": False,
                "action": "error",
                "data": {},
                "response_text": "Please specify which DAG to delete. Example: '/agent delete my_old_dag'"
            }
        
        return {
            "success": True,
            "action": "delete_dag",
            "data": {
                "project_id": str(project_id),
                "dag_id": target
            },
            "response_text": f"🗑️ Are you sure you want to delete DAG **{target}**? This action cannot be undone."
        }
    
    async def _handle_help(
        self, 
        target: str, 
        description: str, 
        project_id: UUID, 
        user_id: str,
        llm_client=None,
        websocket=None,
        start_time=None
    ) -> Dict[str, Any]:
        """Handle /agent help command."""
        help_text = """
🤖 **Agent Commands Help**

Available commands:

📝 **Create DAGs:**
- `/agent create [description]` - Create a new DAG from natural language
  - Example: `/agent create daily data pipeline that processes user analytics`
  - Example: `/agent create ML model training workflow that runs weekly`

📋 **List & View:**
- `/agent list` - List all your DAGs
- `/agent show [dag_name]` - Show details of a specific DAG

✏️ **Edit & Manage:**
- `/agent edit [dag_name]` - Open DAG in visual editor
- `/agent delete [dag_name]` - Delete a DAG

❓ **Help:**
- `/agent help` - Show this help message

**Tips:**
- Be descriptive when creating DAGs - mention what data to process, how often to run, what steps to include
- DAG names are automatically generated from your descriptions
- Use the visual editor to see task dependencies and make complex changes
"""
        
        return {
            "success": True,
            "action": "help",
            "data": {},
            "response_text": help_text
        }
    
    def _determine_complexity(self, description: str) -> str:
        """Determine complexity level from description keywords."""
        description_lower = description.lower()

        # Complex indicators
        complex_keywords = [
            'multiple', 'advanced', 'complex', 'sophisticated', 'integration',
            'real-time', 'streaming', 'ml', 'machine learning', 'ai', 'model',
            'training', 'deployment', 'monitoring', 'alerting', 'notification'
        ]

        # Simple indicators
        simple_keywords = [
            'basic', 'simple', 'straightforward', 'single', 'one-step'
        ]

        complex_count = sum(1 for keyword in complex_keywords if keyword in description_lower)
        simple_count = sum(1 for keyword in simple_keywords if keyword in description_lower)

        if complex_count > simple_count:
            return "complex"
        elif simple_count > complex_count:
            return "simple"
        else:
            return "medium"

    async def _convert_description_to_dag_spec(
        self,
        description: str,
        llm_client=None,
        complexity: str = "medium",
        websocket=None,
        start_time=None
    ) -> Optional[Dict[str, Any]]:
        """
        Convert natural language description to DAG specification using LLM.
        
        This is the key intelligence that transforms user requests like:
        "daily data pipeline that processes user analytics"
        
        Into structured DAG specifications that can be used by the DAG generator.
        """
        
        if not llm_client:
            # Fallback to simple parsing if no LLM available
            return self._fallback_description_parsing(description, complexity)

        try:
            print(f"Generating DAG spec with complexity: {complexity}")
            print(f"LLM client provided: {llm_client is not None}")

            if llm_client:
                print("🔄 Using LLM client for enhanced generation")
                prompt = DagPromptTemplates.get_dag_creation_prompt(description, complexity)
                print(f"Generated enhanced prompt (length: {len(prompt)} chars)")

                # Call LLM to convert description to structured format
                response = await self._call_llm_for_dag_spec(llm_client, prompt, websocket, start_time)
                print(f"LLM response received: {response[:200]}..." if response else "No response from LLM")

                if response:
                    try:
                        parsed_spec = self._parse_llm_response(response)
                        print(f"Parsed enhanced spec: {parsed_spec}")

                        if parsed_spec:
                            print(f"✅ Successfully parsed enhanced DAG spec with {len(parsed_spec.get('tasks', []))} tasks")
                            # The enhanced spec already includes all the required fields
                            return parsed_spec
                        else:
                            print("❌ Failed to parse LLM response, falling back to simple parsing")
                    except ValueError as e:
                        if "Task count mismatch" in str(e):
                            print(f"🔄 Task count mismatch detected, attempting to correct: {e}")
                            # Retry with corrected prompt that emphasizes task count accuracy
                            corrected_prompt = self._get_corrected_prompt(description, complexity, str(e))
                            print("🔄 Retrying with corrected prompt...")
                            response = await self._call_llm_for_dag_spec(llm_client, corrected_prompt, websocket, start_time)
                            if response:
                                try:
                                    parsed_spec = self._parse_llm_response(response)
                                    if parsed_spec:
                                        print(f"✅ Successfully parsed corrected DAG spec with {len(parsed_spec.get('tasks', []))} tasks")
                                        return parsed_spec
                                except ValueError as e2:
                                    print(f"❌ Corrected attempt also failed: {e2}")
                                    # For small mismatches in corrected attempt, auto-correct
                                    if "Task count mismatch too large" not in str(e2):
                                        print("🔄 Attempting to auto-correct small mismatch...")
                                        try:
                                            # Try to parse and auto-correct the response
                                            import json
                                            import re
                                            json_match = re.search(r'\{.*\}', response, re.DOTALL)
                                            if json_match:
                                                json_str = json_match.group()
                                                corrected_spec = json.loads(json_str)
                                                tasks = corrected_spec.get('tasks', [])
                                                estimated = corrected_spec.get('estimated_tasks', 0)
                                                actual = len(tasks)
                                                if abs(estimated - actual) <= 2:
                                                    corrected_spec['estimated_tasks'] = actual
                                                    print(f"✅ Auto-corrected task count: {estimated} → {actual}")
                                                    return corrected_spec
                                        except Exception as auto_correct_error:
                                            print(f"❌ Auto-correction failed: {auto_correct_error}")
                            print("❌ Corrected attempt failed, falling back to simple parsing")
                        else:
                            raise e
                else:
                    print("❌ No response from LLM, falling back to simple parsing")
            else:
                print("❌ No LLM client available, using fallback parsing")

        except Exception as e:
            print(f"LLM conversion failed: {e}")
            fallback_spec = self._fallback_description_parsing(description, complexity)

            # Add error context to the fallback DAG
            if 'error_details' not in fallback_spec:
                fallback_spec['error_details'] = {
                    'original_error': str(e),
                    'fallback_reason': 'LLM generation failed',
                    'timestamp': datetime.now().isoformat()
                }

            # Ensure fallback DAG has all required sections
            fallback_spec = self._ensure_complete_dag_spec(fallback_spec, description, complexity)
            return fallback_spec
    
    def _build_dag_conversion_prompt(self, description: str) -> str:
        """Build prompt for LLM to convert description to DAG spec."""
        return f"""
Convert this natural language description into a structured Airflow DAG specification.

Description: "{description}"

Return a JSON object with this exact structure:
{{
    "dag_name": "descriptive_name_for_dag",
    "description": "Clear description of what this DAG does",
    "schedule_interval": "@daily|@hourly|@weekly|0 0 * * *|etc",
    "owner": "user",
    "tags": ["tag1", "tag2"],
    "tasks": [
        {{
            "task_id": "task_name",
            "task_type": "python|bash",
            "python_code": "print('Python code here')" or null,
            "bash_command": "echo 'Bash command here'" or null,
            "dependencies": ["previous_task_id"],
            "provide_context": true|false
        }}
    ]
}}

Guidelines:
- Infer appropriate schedule from description (daily/hourly/weekly)
- Break down the workflow into logical tasks
- Use Python tasks for data processing, bash for system commands
- Create reasonable task dependencies
- Keep task IDs short and descriptive
- Add relevant tags based on the domain (etl, ml, analytics, etc.)

Return ONLY the JSON object, no other text.
"""
    
    async def _call_llm_for_dag_spec(self, llm_client, prompt: str, websocket=None, start_time=None) -> str:
        """Call LLM with prompt and return response, optionally providing progress updates."""
        if not llm_client:
            # Fallback if no client available
            return '{"dag_name": "fallback_dag", "description": "Generated via fallback", "schedule_interval": "@daily", "owner": "user", "tags": ["fallback"], "tasks": []}'

        try:
            # Track LLM operation performance
            with track_llm_operation(len(prompt)):
                # Send initial progress update if WebSocket available
                if websocket and start_time:
                    print(f"🔄 Sending LLM progress update: Analysis starting")
                    await websocket.send_json({
                        "type": "progress",
                        "message": "🤖 AI is analyzing your request...",
                        "stage": "llm_analysis", 
                        "progress": 8,
                        "elapsed_time": time.time() - start_time,
                        "timestamp": time.time()
                    })
                else:
                    print(f"🚫 Cannot send progress - WebSocket: {websocket is not None}, start_time: {start_time is not None}")

                # Start generating response with periodic progress updates
                import asyncio
                
                # Create task for LLM generation
                generation_task = asyncio.create_task(llm_client.generate(prompt))
                
                # Send progress updates while waiting
                if websocket and start_time:
                    progress_messages = [
                        (3, "🧠 Processing natural language description..."),
                        (6, "🔍 Identifying workflow patterns..."),
                        (9, "⚙️ Designing task structure..."),
                        (12, "📝 Generating production-ready code..."),
                    ]
                    
                    for delay, (progress, message) in enumerate(progress_messages, 1):
                        try:
                            # Wait for either the task to complete or timeout
                            await asyncio.wait_for(asyncio.shield(generation_task), timeout=delay * 1.5)
                            break  # Task completed
                        except asyncio.TimeoutError:
                            # Task still running, send progress update
                            if not generation_task.done():
                                print(f"🔄 Sending LLM progress update: {message} ({progress}%)")
                                await websocket.send_json({
                                    "type": "progress", 
                                    "message": message,
                                    "stage": "llm_generation",
                                    "progress": progress,
                                    "elapsed_time": time.time() - start_time,
                                    "timestamp": time.time()
                                })
                else:
                    print(f"🚫 Cannot send periodic progress - WebSocket: {websocket is not None}, start_time: {start_time is not None}")

                # Wait for completion
                response = await generation_task
                
                if response:
                    logger.info(f"LLM response received: {len(response)} characters")
                    
                    # Send completion update
                    if websocket and start_time:
                        print(f"🔄 Sending LLM completion update: 15%")
                        await websocket.send_json({
                            "type": "progress",
                            "message": "✅ AI specification complete, validating...",
                            "stage": "llm_complete",
                            "progress": 15,
                            "elapsed_time": time.time() - start_time,
                            "timestamp": time.time()
                        })
                    
                    return response
                else:
                    logger.warning("LLM returned empty response, using fallback")
                    return '{"dag_name": "llm_error_dag", "description": "LLM generation failed", "schedule_interval": "@daily", "owner": "user", "tags": ["error"], "tasks": []}'
        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return '{"dag_name": "llm_error_dag", "description": "LLM call failed", "schedule_interval": "@daily", "owner": "user", "tags": ["error"], "tasks": []}'
    
    def _parse_llm_response(self, response: str) -> Optional[Dict[str, Any]]:
        """Parse LLM response into DAG specification with robust error handling."""
        try:
            # Use the robust parsing function that tries multiple strategies
            parsed_spec = self._parse_json_robustly(response)

            if parsed_spec:
                # Apply standard validation and corrections
                validated_spec = self._validate_and_correct_spec(parsed_spec)
                
                # Verify tasks are meaningful, not placeholders
                if self._has_meaningful_tasks(validated_spec):
                    return validated_spec
                else:
                    logger.warning("LLM generated placeholder tasks, will enhance with smart fallback")
                    # Enhance the spec with better tasks based on the description
                    return self._enhance_with_smart_tasks(validated_spec)
            else:
                logger.error("All JSON parsing strategies failed")
                return None

        except Exception as e:
            logger.error(f"Error parsing LLM response: {e}")
            return None

    def _validate_and_correct_spec(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and apply corrections to a parsed DAG spec."""
        # Ensure dag_id exists
        if 'dag_id' not in spec:
            from .dag_naming import generate_smart_dag_name
            spec['dag_id'] = generate_smart_dag_name(spec.get('description', ''), 'medium', None)

        # Validate estimated_tasks matches actual task count
        tasks = spec.get('tasks', [])
        estimated_tasks = spec.get('estimated_tasks', 0)
        actual_tasks = len(tasks)

        if estimated_tasks != actual_tasks:
            logger.warning(f"Task count mismatch: estimated={estimated_tasks}, actual={actual_tasks}")
            # For small mismatches, auto-correct
            if abs(estimated_tasks - actual_tasks) <= 2:
                logger.info(f"Auto-correcting task count mismatch: {estimated_tasks} → {actual_tasks}")
                spec['estimated_tasks'] = actual_tasks

        # Ensure all required sections are present
        spec = self._ensure_complete_dag_spec(spec, spec.get('description', ''), spec.get('complexity', 'medium'))

        logger.info(f"Successfully parsed and validated DAG spec with {len(tasks)} tasks")
        return spec

    def _parse_standard_llm_response(self, response: str) -> Optional[Dict[str, Any]]:
        """Parse standard-size LLM responses."""
        try:
            # Clean the response first
            response = self._clean_llm_response(response)

            # Extract JSON from response (handle cases where LLM adds extra text)
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group()
                spec = json.loads(json_str)

                # Validate required fields
                required_fields = ['dag_name', 'description', 'schedule_interval', 'tasks']
                if all(field in spec for field in required_fields):
                    # Ensure dag_id exists
                    if 'dag_id' not in spec:
                        from .dag_naming import generate_smart_dag_name
                        spec['dag_id'] = generate_smart_dag_name(spec['description'], 'medium', None)

                    # Validate estimated_tasks matches actual task count
                    tasks = spec.get('tasks', [])
                    estimated_tasks = spec.get('estimated_tasks', 0)
                    actual_tasks = len(tasks)
                    if estimated_tasks != actual_tasks:
                        logger.warning(f"Task count mismatch: estimated={estimated_tasks}, actual={actual_tasks}")
                        # For small mismatches (1-2 tasks), auto-correct instead of failing
                        if abs(estimated_tasks - actual_tasks) <= 2:
                            logger.info(f"Auto-correcting task count mismatch: {estimated_tasks} → {actual_tasks}")
                            spec['estimated_tasks'] = actual_tasks
                        else:
                            # For larger mismatches, still raise error to force LLM correction
                            raise ValueError(f"Task count mismatch too large: LLM estimated {estimated_tasks} tasks but generated {actual_tasks}. This indicates inconsistent planning and generation phases.")

                    logger.info(f"Successfully parsed DAG spec with {len(tasks)} tasks")
                    return spec

            logger.warning("No valid JSON found in LLM response")
            return None
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error(f"Failed to parse JSON from LLM response: {e}")
            return None

    def _parse_large_llm_response(self, response: str) -> Optional[Dict[str, Any]]:
        """Parse large LLM responses with memory-efficient chunked processing."""
        try:
            logger.debug("Using chunked processing for large LLM response")

            # Extract JSON portion only to reduce memory usage
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if not json_match:
                logger.error("No JSON object found in large LLM response")
                return None

            json_str = json_match.group()
            json_length = len(json_str)

            # For very large JSON, log size for monitoring
            if json_length > 10000:
                logger.info(f"Processing very large JSON response ({json_length} chars)")

            # Parse with memory monitoring
            spec = json.loads(json_str)

            # Validate required fields
            required_fields = ['dag_name', 'description', 'schedule_interval', 'tasks']
            if not all(field in spec for field in required_fields):
                logger.error("Large LLM response missing required fields")
                return None

            # Ensure dag_id exists
            if 'dag_id' not in spec:
                from .dag_naming import generate_smart_dag_name
                spec['dag_id'] = generate_smart_dag_name(spec['description'], 'medium', None)

            # Validate tasks array size for memory safety
            tasks = spec.get('tasks', [])
            if len(tasks) > 50:  # Arbitrary limit to prevent memory issues
                logger.warning(f"Very large number of tasks ({len(tasks)}), may impact performance")

            # Validate estimated_tasks matches actual task count
            estimated_tasks = spec.get('estimated_tasks', 0)
            actual_tasks = len(tasks)
            if estimated_tasks != actual_tasks:
                logger.warning(f"Task count mismatch: estimated={estimated_tasks}, actual={actual_tasks}")
                # For small mismatches (1-2 tasks), auto-correct instead of failing
                if abs(estimated_tasks - actual_tasks) <= 2:
                    logger.info(f"Auto-correcting task count mismatch: {estimated_tasks} → {actual_tasks}")
                    spec['estimated_tasks'] = actual_tasks
                else:
                    # For larger mismatches, still raise error to force LLM correction
                    raise ValueError(f"Task count mismatch too large: LLM estimated {estimated_tasks} tasks but generated {actual_tasks}. This indicates inconsistent planning and generation phases.")

            logger.info(f"Successfully parsed large DAG spec with {len(tasks)} tasks")
            return spec

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from large LLM response: {e}")
            return None
        except MemoryError:
            logger.error("Memory error while parsing large LLM response")
            return None

    def _get_corrected_prompt(self, description: str, complexity: str, error_message: str) -> str:
        """Generate a corrected prompt when task count mismatch occurs."""
        return f"""
CRITICAL CORRECTION REQUIRED: {error_message}

The previous attempt had a task count mismatch. You MUST ensure that:
1. "estimated_tasks" exactly equals the number of tasks in the "tasks" array
2. Count your tasks carefully BEFORE setting estimated_tasks
3. Double-check the final count matches

Original Description: "{description}"

**MANDATORY TASK COUNTING PROCESS:**
1. First, analyze the workflow and list ALL required tasks on paper
2. Count each distinct task (do not skip any)
3. Set "estimated_tasks" to exactly that number
4. Create exactly that many tasks in the "tasks" array
5. Verify: estimated_tasks == len(tasks)

**TASK COUNT VALIDATION CHECKLIST:**
- [] Extract data from source
- [] Validate/transform data
- [] Load data to destination
- [] Send notifications/alerts
- [] Handle errors/cleanup
- [] Any domain-specific tasks

Return a JSON object with this EXACT structure:
{{
    "dag_name": "Properly Capitalized Descriptive Name",
    "dag_id": "snake_case_machine_identifier",
    "description": "Clear, comprehensive description",
    "schedule_interval": "@daily",
    "owner": "data-team",
    "tags": ["domain", "frequency", "data-type"],
    "complexity": "{complexity}",
    "estimated_tasks": EXACT_COUNT_BASED_ON_YOUR_ANALYSIS,
    "tasks": [
        // EXACTLY estimated_tasks number of tasks here
        {{
            "task_id": "task_1",
            "task_type": "python",
            "description": "Description of task 1",
            "python_code": "# Production-ready Python code with comprehensive error handling\\n    from airflow.models import Variable\\n    from airflow.exceptions import AirflowException\\n    import logging\\n    import os\\n    \\n    logger = logging.getLogger(__name__)\\n    \\n    def validate_inputs(**kwargs):\\n        \\\"\\\"\\\"Validate task inputs and dependencies.\\\"\\\"\\\"\\n        ti = kwargs['ti']\\n        # Check if previous tasks completed successfully\\n        return True\\n    \\n    def perform_task_operation(source_path, **kwargs):\\n        \\\"\\\"\\\"Main task operation with proper error handling.\\\"\\\"\\\"\\n        if not os.path.exists(source_path):\\n            raise FileNotFoundError(f'Source path does not exist: {{source_path}}')\\n        result = {{\\\"processed_files\\\": 1, \\\"status\\\": \\\"completed\\\"}}\\n        return result\\n    \\n    try:\\n        if not validate_inputs(**kwargs):\\n            raise AirflowException('Input validation failed')\\n        \\n        source_path = Variable.get('source_directory', '/default/source')\\n        backup_path = Variable.get('backup_directory', '/default/backup')\\n        \\n        logger.info(f'Starting task execution')\\n        logger.info(f'Source: {{source_path}}, Backup: {{backup_path}}')\\n        \\n        if not os.path.exists(os.path.dirname(backup_path)):\\n            os.makedirs(os.path.dirname(backup_path), exist_ok=True)\\n            logger.info(f'Created backup directory: {{os.path.dirname(backup_path)}}')\\n        \\n        result = perform_task_operation(source_path, **kwargs)\\n        \\n        logger.info(f'Task completed successfully: {{result}}')\\n        return result\\n        \\n    except FileNotFoundError as e:\\n        logger.error(f'File not found: {{str(e)}}')\\n        raise AirflowException(f'File operation failed: {{str(e)}}')\\n    except PermissionError as e:\\n        logger.error(f'Permission denied: {{str(e)}}')\\n        raise AirflowException(f'Permission error: {{str(e)}}')\\n    except Exception as e:\\n        logger.error(f'Task failed with unexpected error: {{str(e)}}')\\n        logger.error(f'Error type: {{type(e).__name__}}')\\n        import traceback\\n        logger.error(f'Traceback: {{traceback.format_exc()}}')\\n        raise AirflowException(f'Task execution failed: {{str(e)}}')",
            "dependencies": [],
            "provide_context": true,
            "retries": 2,
            "retry_delay_minutes": 5,
            "execution_timeout_minutes": 30
        }}
        // ... more tasks to match estimated_tasks count
    ],
    "directory_structure": {{
        "base_directory": "dags/dag_id/",
        "main_file": "dags/dag_id/dag_id.py",
        "config_file": "dags/dag_id/config.json",
        "readme_file": "dags/dag_id/README.md",
        "utils_directory": "dags/dag_id/utils/",
        "utils_init": "dags/dag_id/utils/__init__.py",
        "utils_helpers": "dags/dag_id/utils/helpers.py"
    }},
    "supabase_paths": {{
        "main_file": "projects/{{project_id}}/dags/dag_id/dag_id.py",
        "config_file": "projects/{{project_id}}/dags/dag_id/config.json",
        "readme_file": "projects/{{project_id}}/dags/dag_id/README.md",
        "utils_init": "projects/{{project_id}}/dags/dag_id/utils/__init__.py",
        "utils_helpers": "projects/{{project_id}}/dags/dag_id/utils/helpers.py"
    }},
    "validation_checks": {{
        "reasonable_task_count": true,
        "logical_dependencies": true,
        "proper_naming": true,
        "complete_structure": true,
        "no_circular_dependencies": true,
        "airflow_compatible": true,
        "supabase_ready": true
    }},
    "airflow_metadata": {{
        "dag_file": "dag_id.py",
        "config_file": "config.json",
        "start_date": "2024-01-01",
        "concurrency": 1,
        "max_active_runs": 1,
        "dagrun_timeout_hours": 2,
        "catchup": false
    }}
}}

**FINAL VALIDATION STEP:**
Before submitting, manually count the tasks in your "tasks" array and ensure estimated_tasks equals that exact count.

Return ONLY the JSON object, no other text.
"""

    async def _refine_dag_spec(self, dag_spec: Dict[str, Any], issues: List[str], llm_client, websocket=None, start_time=None) -> Optional[Dict[str, Any]]:
        """Refine a DAG specification based on validation issues."""
        try:
            prompt = DagPromptTemplates.get_dag_refinement_prompt(dag_spec, issues)

            # Call LLM to refine the DAG spec  
            response = await self._call_llm_for_dag_spec(llm_client, prompt, websocket, start_time)

            refined_spec = self._parse_llm_response(response)
            if refined_spec:
                # Add complexity and validation info to the refined spec
                refined_spec["complexity"] = dag_spec.get("complexity", "medium")
                refined_spec["estimated_tasks"] = len(refined_spec.get("tasks", []))
                refined_spec["directory_structure"] = {
                    "main_file": f"dags/{refined_spec['dag_id']}/{refined_spec['dag_id']}.py",
                    "config_file": f"dags/{refined_spec['dag_id']}/config.json",
                    "readme_file": f"dags/{refined_spec['dag_id']}/README.md",
                    "utils_directory": f"dags/{refined_spec['dag_id']}/utils/"
                }
                refined_spec["validation_checks"] = {
                    "reasonable_task_count": True,
                    "logical_dependencies": True,
                    "proper_naming": True,
                    "complete_structure": True
                }

            return refined_spec

        except Exception as e:
            print(f"DAG refinement failed: {e}")
            return None
    
    def _fallback_description_parsing(self, description: str, complexity: str = "medium") -> Dict[str, Any]:
        """
        Improved fallback parsing when LLM is not available.
        Parses specific task descriptions from natural language using enhanced NLP patterns.
        """
        description_lower = description.lower()

        # Determine schedule with more sophisticated parsing
        schedule = self._extract_schedule_from_description(description_lower)

        # Use smart naming for the DAG
        from .dag_naming import generate_smart_dag_name
        dag_name = generate_smart_dag_name(description, complexity)
        dag_id = dag_name  # The smart name is already sanitized

        # Determine tags based on keywords
        tags = self._extract_tags(description_lower)

        # Enhanced task parsing with multiple strategies
        tasks = self._parse_tasks_from_description_enhanced(description)

        # If no specific tasks found, fall back to smart pattern matching
        if not tasks:
            tasks = self._create_smart_fallback_tasks(description, description_lower)

        # Set up intelligent dependencies
        if tasks:
            tasks = self._create_intelligent_dependencies(tasks, description_lower)

        return {
            "dag_name": dag_name,
            "dag_id": dag_id,
            "description": description,
            "schedule_interval": schedule,
            "owner": "data-team",
            "tags": tags,
            "complexity": complexity,
            "estimated_tasks": len(tasks),
            "tasks": tasks,
            "directory_structure": {
                "main_file": f"dags/{dag_id}/{dag_id}.py",
                "config_file": f"dags/{dag_id}/config.json",
                "readme_file": f"dags/{dag_id}/README.md",
                "utils_directory": f"dags/{dag_id}/utils/"
            },
            "validation_checks": {
                "reasonable_task_count": True,
                "logical_dependencies": True,
                "proper_naming": True,
                "complete_structure": True
            }
        }

    def _extract_dag_name(self, description: str) -> str:
        """Extract DAG name from description, handling various formats."""
        # Remove common prefixes
        name_part = description
        if 'dag name' in description.lower():
            # Extract name after "dag name"
            parts = description.lower().split('dag name')
            if len(parts) > 1:
                name_part = parts[1].split(',')[0].strip()

        # Clean and format the name
        dag_name = re.sub(r'[^a-zA-Z0-9\s]', '', name_part)
        dag_name = '_'.join(dag_name.lower().split()[:4])  # Take first 4 words

        # Fallback if name is too short
        if len(dag_name) < 3:
            dag_name = f"custom_dag_{hash(description) % 1000}"

        return dag_name

    def _extract_tags(self, description_lower: str) -> List[str]:
        """Extract relevant tags from description."""
        tags = []
        if any(word in description_lower for word in ['data', 'etl', 'pipeline']):
            tags.append('data')
        if any(word in description_lower for word in ['ml', 'model', 'train', 'machine learning']):
            tags.append('ml')
        if any(word in description_lower for word in ['analytics', 'report', 'dashboard']):
            tags.append('analytics')
        if any(word in description_lower for word in ['notification', 'alert', 'email']):
            tags.append('notification')
        return tags

    def _extract_schedule_from_description(self, description_lower: str) -> str:
        """Extract schedule interval from description with sophisticated parsing."""
        # Check for specific time patterns
        time_patterns = [
            (r'every (\d+) hour', lambda m: f"0 */{m.group(1)} * * *"),
            (r'hourly|every hour', "@hourly"),
            (r'daily|every day', "@daily"),
            (r'weekly|every week', "@weekly"),
            (r'monthly|every month', "@monthly"),
            (r'at (\d{1,2}):(\d{2})', lambda m: f"{m.group(2)} {m.group(1)} * * *"),
            (r'at (\d{1,2}) (am|pm)', lambda m: f"0 {m.group(1)} * * *"),
            (r'every morning|morning', "0 6 * * *"),  # 6 AM
            (r'every evening|evening', "0 18 * * *"),  # 6 PM
            (r'midnight', "0 0 * * *"),
            (r'noon', "0 12 * * *"),
        ]
        
        for pattern, schedule in time_patterns:
            match = re.search(pattern, description_lower)
            if match:
                if callable(schedule):
                    return schedule(match)
                return schedule
        
        return "@daily"  # default

    def _parse_tasks_from_description_enhanced(self, description: str) -> List[Dict[str, Any]]:
        """Enhanced task parsing with multiple NLP strategies."""
        tasks = []
        description_lower = description.lower()
        
        # Strategy 1: Look for explicit task mentions
        task_patterns = [
            # "task 1: do something, task 2: do something else"
            r'task\s+(\d+|\w+)\s*:\s*([^,\n]+)',
            # "step 1: do something, step 2: do something else"
            r'step\s+(\d+|\w+)\s*:\s*([^,\n]+)',
            # "first extract, then transform, finally load"
            r'(?:first|then|next|after|finally)\s+([^,\n]+)',
            # Bullet points "- extract data"
            r'[-•]\s*([^,\n]+)',
        ]
        
        for pattern in task_patterns:
            matches = re.findall(pattern, description, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    if len(match) == 2:
                        task_num, action = match
                        task_id = self._generate_task_id_from_action(action.strip())
                        task = self._create_task_from_action_enhanced(task_id, action.strip())
                        tasks.append(task)
                    else:
                        action = match[0] if match else ""
                        task_id = self._generate_task_id_from_action(action.strip())
                        task = self._create_task_from_action_enhanced(task_id, action.strip())
                        tasks.append(task)
                else:
                    action = match
                    task_id = self._generate_task_id_from_action(action.strip())
                    task = self._create_task_from_action_enhanced(task_id, action.strip())
                    tasks.append(task)
        
        # Strategy 2: Look for workflow verbs (even if we found some tasks, we might miss some)
        workflow_verbs = [
            ('extract', 'data extraction'),
            ('fetch', 'data fetching'),
            ('process', 'data processing'),
            ('transform', 'data transformation'),
            ('clean', 'data cleaning'),
            ('validate', 'data validation'),
            ('load', 'data loading'),
            ('save', 'data saving'),
            ('upload', 'data upload'),
            ('download', 'data download'),
            ('analyze', 'data analysis'),
            ('aggregate', 'data aggregation'),
            ('summarize', 'data summarization'),
            ('report', 'report generation'),
            ('notify', 'notification'),
            ('notification', 'notification'),
            ('alert', 'alerting'),
            ('backup', 'backup creation'),
            ('archive', 'data archiving'),
            ('sync', 'data synchronization'),
            ('monitor', 'monitoring'),
            ('train', 'model training'),
            ('deploy', 'model deployment'),
            ('test', 'testing'),
            ('verify', 'verification'),
        ]
        
        # Find verbs that aren't already covered by existing tasks
        existing_task_ids = [t['task_id'].lower() for t in tasks]
        found_verbs = []
        
        for verb, description_text in workflow_verbs:
            if verb in description_lower:
                # Check if we already have a task covering this verb
                verb_covered = any(verb in task_id for task_id in existing_task_ids)
                if not verb_covered:
                    found_verbs.append((verb, description_text))
        
        # Create tasks for found verbs
        for i, (verb, desc) in enumerate(found_verbs):
            task_id = f"{verb}_data" if verb not in ['notify', 'alert', 'backup', 'monitor', 'train', 'deploy', 'test', 'verify'] else verb
            task = self._create_task_from_action_enhanced(task_id, desc)
            tasks.append(task)
        
        return tasks

    def _generate_task_id_from_action(self, action: str) -> str:
        """Generate a meaningful task ID from an action description."""
        # Clean and extract key words
        action = re.sub(r'[^\w\s]', ' ', action.lower())
        words = [w for w in action.split() if len(w) > 2 and w not in ['the', 'and', 'from', 'with', 'for']]
        
        # Take first 3 meaningful words
        key_words = words[:3]
        task_id = '_'.join(key_words)
        
        # Ensure it's a valid task ID
        task_id = re.sub(r'[^a-z0-9_]', '_', task_id)
        if not task_id or task_id[0].isdigit():
            task_id = f"task_{task_id}"
        
        return task_id[:30]  # Limit length

    def _create_task_from_action_enhanced(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create a sophisticated task based on the parsed action."""
        action_lower = action.lower()
        
        # Determine task type and generate meaningful code
        if any(keyword in action_lower for keyword in ['extract', 'fetch', 'get', 'pull', 'read']):
            return self._create_extraction_task(task_id, action)
        elif any(keyword in action_lower for keyword in ['transform', 'process', 'clean', 'parse', 'convert']):
            return self._create_transformation_task(task_id, action)
        elif any(keyword in action_lower for keyword in ['load', 'save', 'store', 'write', 'upload']):
            return self._create_loading_task(task_id, action)
        elif any(keyword in action_lower for keyword in ['validate', 'check', 'verify', 'test']):
            return self._create_validation_task(task_id, action)
        elif any(keyword in action_lower for keyword in ['notify', 'alert', 'email', 'message']):
            return self._create_notification_task(task_id, action)
        elif any(keyword in action_lower for keyword in ['analyze', 'calculate', 'compute', 'summarize']):
            return self._create_analysis_task(task_id, action)
        elif any(keyword in action_lower for keyword in ['backup', 'archive', 'sync', 'replicate']):
            return self._create_backup_task(task_id, action)
        else:
            return self._create_generic_task_enhanced(task_id, action)

    def _create_extraction_task(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create a data extraction task."""
        return {
            "task_id": task_id,
            "task_type": "python",
            "description": f"Extract data - {action}",
            "python_code": f"""# {action}
import logging
from airflow.models import Variable
import pandas as pd

logger = logging.getLogger(__name__)

try:
    # Get source configuration
    source_config = Variable.get('source_config', default_var='{{"type": "database", "connection": "default"}}')
    logger.info(f'Starting data extraction: {action}')
    
    # TODO: Implement specific extraction logic based on source type
    # Example for database:
    # connection_string = Variable.get('database_connection_string')
    # query = Variable.get('extraction_query', 'SELECT * FROM source_table')
    # df = pd.read_sql(query, connection_string)
    
    # Example for API:
    # api_endpoint = Variable.get('api_endpoint')
    # response = requests.get(api_endpoint)
    # data = response.json()
    
    # Example for file:
    # file_path = Variable.get('source_file_path')
    # df = pd.read_csv(file_path)
    
    # Placeholder result
    extracted_data = {{'records': 1000, 'status': 'extracted'}}
    
    logger.info(f'Extraction completed: {{extracted_data["records"]}} records')
    return extracted_data
    
except Exception as e:
    logger.error(f'Data extraction failed: {{str(e)}}')
    raise""",
            "dependencies": [],
            "provide_context": True,
            "retries": 2,
            "retry_delay_minutes": 5,
            "execution_timeout_minutes": 30
        }

    def _create_transformation_task(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create a data transformation task."""
        return {
            "task_id": task_id,
            "task_type": "python",
            "description": f"Transform data - {action}",
            "python_code": f"""# {action}
import logging
import pandas as pd
from airflow.models import Variable

logger = logging.getLogger(__name__)

try:
    # Get transformation configuration
    transform_config = Variable.get('transform_config', default_var='{{"rules": []}}')
    logger.info(f'Starting data transformation: {action}')
    
    # TODO: Get data from previous extraction task
    # ti = kwargs['ti']
    # input_data = ti.xcom_pull(task_ids='extract_task')
    
    # Example transformation operations:
    # - Data type conversions
    # - Column renaming
    # - Data cleaning and validation
    # - Aggregations and calculations
    # - Filtering and sorting
    
    # Placeholder transformation
    transformed_data = {{
        'processed_records': 950,
        'transformation_applied': '{action}',
        'data_quality_score': 0.95
    }}
    
    logger.info(f'Transformation completed: {{transformed_data["processed_records"]}} records processed')
    return transformed_data
    
except Exception as e:
    logger.error(f'Data transformation failed: {{str(e)}}')
    raise""",
            "dependencies": [],
            "provide_context": True,
            "retries": 2,
            "retry_delay_minutes": 3,
            "execution_timeout_minutes": 25
        }

    def _create_loading_task(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create a data loading task."""
        return {
            "task_id": task_id,
            "task_type": "python",
            "description": f"Load data - {action}",
            "python_code": f"""# {action}
import logging
from airflow.models import Variable
import sqlalchemy as sa

logger = logging.getLogger(__name__)

try:
    # Get destination configuration
    dest_config = Variable.get('destination_config', default_var='{{"type": "database"}}')
    logger.info(f'Starting data loading: {action}')
    
    # TODO: Get transformed data from previous task
    # ti = kwargs['ti']
    # data_to_load = ti.xcom_pull(task_ids='transform_task')
    
    # Example loading operations:
    # Database loading:
    # connection_string = Variable.get('destination_connection_string')
    # engine = sa.create_engine(connection_string)
    # df.to_sql('target_table', engine, if_exists='append', index=False)
    
    # File loading:
    # output_path = Variable.get('output_file_path')
    # df.to_csv(output_path, index=False)
    
    # API loading:
    # api_endpoint = Variable.get('destination_api_endpoint')
    # response = requests.post(api_endpoint, json=data_to_load)
    
    loaded_result = {{
        'loaded_records': 950,
        'destination': 'target_system',
        'load_timestamp': '2024-01-01T00:00:00Z'
    }}
    
    logger.info(f'Loading completed: {{loaded_result["loaded_records"]}} records loaded')
    return loaded_result
    
except Exception as e:
    logger.error(f'Data loading failed: {{str(e)}}')
    raise""",
            "dependencies": [],
            "provide_context": True,
            "retries": 3,
            "retry_delay_minutes": 10,
            "execution_timeout_minutes": 45
        }

    def _create_validation_task(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create a data validation task."""
        return {
            "task_id": task_id,
            "task_type": "python",
            "description": f"Validate data - {action}",
            "python_code": f"""# {action}
import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)

try:
    # Get validation rules
    validation_rules = Variable.get('validation_rules', default_var='{{"required_fields": [], "data_types": {{}}}}')
    logger.info(f'Starting data validation: {action}')
    
    # TODO: Get data to validate
    # ti = kwargs['ti']
    # data_to_validate = ti.xcom_pull(task_ids='previous_task')
    
    # Example validation checks:
    # - Required field presence
    # - Data type validation
    # - Range and constraint checks
    # - Business rule validation
    # - Data quality metrics
    
    validation_result = {{
        'total_records': 1000,
        'valid_records': 987,
        'invalid_records': 13,
        'validation_errors': [
            'Missing required field: email',
            'Invalid date format in created_at'
        ],
        'data_quality_score': 0.987
    }}
    
    # Log validation results
    logger.info(f'Validation completed: {{validation_result["valid_records"]}}/{{validation_result["total_records"]}} records valid')
    
    if validation_result['data_quality_score'] < 0.95:
        logger.warning(f'Data quality below threshold: {{validation_result["data_quality_score"]}}')
    
    return validation_result
    
except Exception as e:
    logger.error(f'Data validation failed: {{str(e)}}')
    raise""",
            "dependencies": [],
            "provide_context": True,
            "retries": 1,
            "retry_delay_minutes": 2,
            "execution_timeout_minutes": 15
        }

    def _create_notification_task(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create a notification task."""
        return {
            "task_id": task_id,
            "task_type": "python",
            "description": f"Send notification - {action}",
            "python_code": f"""# {action}
import logging
from airflow.models import Variable
import requests

logger = logging.getLogger(__name__)

try:
    # Get notification configuration
    notification_config = Variable.get('notification_config', default_var='{{"type": "email", "recipients": []}}')
    logger.info(f'Starting notification: {action}')
    
    # TODO: Get results from previous tasks to include in notification
    # ti = kwargs['ti']
    # task_results = ti.xcom_pull(task_ids=['extract_task', 'transform_task', 'load_task'])
    
    # Example notification types:
    # Email notification:
    # send_email_notification(recipients, subject, content)
    
    # Slack notification:
    # slack_webhook = Variable.get('slack_webhook_url')
    # requests.post(slack_webhook, json={{'text': 'Workflow completed successfully'}})
    
    # Teams notification:
    # teams_webhook = Variable.get('teams_webhook_url')
    # requests.post(teams_webhook, json={{'text': 'Pipeline execution finished'}})
    
    notification_result = {{
        'notification_type': 'success',
        'recipients_notified': 3,
        'notification_sent': True,
        'timestamp': '2024-01-01T00:00:00Z'
    }}
    
    logger.info(f'Notification sent successfully to {{notification_result["recipients_notified"]}} recipients')
    return notification_result
    
except Exception as e:
    logger.error(f'Notification failed: {{str(e)}}')
    # Don't raise for notifications - log error but don't fail the pipeline
    return {{'notification_sent': False, 'error': str(e)}}""",
            "dependencies": [],
            "provide_context": True,
            "retries": 1,
            "retry_delay_minutes": 2,
            "execution_timeout_minutes": 10
        }

    def _create_analysis_task(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create a data analysis task."""
        return {
            "task_id": task_id,
            "task_type": "python",
            "description": f"Analyze data - {action}",
            "python_code": f"""# {action}
import logging
import pandas as pd
import numpy as np
from airflow.models import Variable

logger = logging.getLogger(__name__)

try:
    # Get analysis configuration
    analysis_config = Variable.get('analysis_config', default_var='{{"metrics": [], "aggregations": []}}')
    logger.info(f'Starting data analysis: {action}')
    
    # TODO: Get data to analyze
    # ti = kwargs['ti']
    # data_to_analyze = ti.xcom_pull(task_ids='previous_task')
    
    # Example analysis operations:
    # - Statistical calculations (mean, median, std)
    # - Trend analysis
    # - Correlation analysis
    # - Outlier detection
    # - Business metrics calculation
    
    analysis_result = {{
        'total_records_analyzed': 1000,
        'metrics': {{
            'average_value': 125.67,
            'median_value': 120.00,
            'std_deviation': 23.45,
            'min_value': 45.12,
            'max_value': 289.33
        }},
        'trends': {{
            'growth_rate': 0.15,
            'trend_direction': 'upward'
        }},
        'insights': [
            'Data shows 15% growth over previous period',
            'No significant outliers detected',
            'Normal distribution pattern observed'
        ]
    }}
    
    logger.info(f'Analysis completed: {{len(analysis_result["insights"])}} insights generated')
    return analysis_result
    
except Exception as e:
    logger.error(f'Data analysis failed: {{str(e)}}')
    raise""",
            "dependencies": [],
            "provide_context": True,
            "retries": 2,
            "retry_delay_minutes": 5,
            "execution_timeout_minutes": 20
        }

    def _create_backup_task(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create a backup/archival task."""
        return {
            "task_id": task_id,
            "task_type": "python",
            "description": f"Backup/Archive - {action}",
            "python_code": f"""# {action}
import logging
from airflow.models import Variable
import shutil
import os
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    # Get backup configuration
    backup_config = Variable.get('backup_config', default_var='{{"destination": "s3", "retention_days": 30}}')
    logger.info(f'Starting backup operation: {action}')
    
    # Generate backup timestamp
    backup_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # TODO: Get data or files to backup
    # source_path = Variable.get('backup_source_path')
    # destination_path = Variable.get('backup_destination_path')
    
    # Example backup operations:
    # File system backup:
    # backup_path = f'{{destination_path}}/backup_{{backup_timestamp}}'
    # shutil.copytree(source_path, backup_path)
    
    # Database backup:
    # backup_file = f'db_backup_{{backup_timestamp}}.sql'
    # os.system(f'pg_dump database_name > {{backup_file}}')
    
    # Cloud storage backup:
    # upload_to_s3(source_files, f's3://backup-bucket/{{backup_timestamp}}/')
    
    backup_result = {{
        'backup_created': True,
        'backup_timestamp': backup_timestamp,
        'backup_size_mb': 234.56,
        'files_backed_up': 1250,
        'backup_location': f'backup_{{backup_timestamp}}'
    }}
    
    logger.info(f'Backup completed: {{backup_result["files_backed_up"]}} files backed up')
    return backup_result
    
except Exception as e:
    logger.error(f'Backup operation failed: {{str(e)}}')
    raise""",
            "dependencies": [],
            "provide_context": True,
            "retries": 2,
            "retry_delay_minutes": 10,
            "execution_timeout_minutes": 60
        }

    def _create_generic_task_enhanced(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create an enhanced generic task."""
        return {
            "task_id": task_id,
            "task_type": "python",
            "description": f"Execute workflow step - {action}",
            "python_code": f"""# {action}
import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)

try:
    logger.info(f'Starting task: {action}')
    
    # Get task-specific configuration
    task_config = Variable.get('{task_id}_config', default_var='{{"enabled": true}}')
    
    # TODO: Implement the specific logic for: {action}
    # This task should be customized based on your specific requirements
    
    # Example workflow operations you might implement:
    # - File processing
    # - API calls
    # - Data manipulation
    # - System operations
    # - Business logic execution
    
    result = {{
        'task_id': '{task_id}',
        'action_performed': '{action}',
        'status': 'completed',
        'execution_time': '2024-01-01T00:00:00Z',
        'details': 'Task executed successfully'
    }}
    
    logger.info(f'Task {{result["task_id"]}} completed: {{result["details"]}}')
    return result
    
except Exception as e:
    logger.error(f'Task {task_id} failed: {{str(e)}}')
    raise""",
            "dependencies": [],
            "provide_context": True,
            "retries": 2,
            "retry_delay_minutes": 5,
            "execution_timeout_minutes": 30
        }

    def _create_intelligent_dependencies(self, tasks: List[Dict[str, Any]], description_lower: str) -> List[Dict[str, Any]]:
        """Create intelligent task dependencies based on task types and description."""
        if len(tasks) <= 1:
            return tasks
        
        # Common workflow patterns
        workflow_patterns = {
            'etl': ['extract', 'transform', 'load'],
            'data_pipeline': ['extract', 'validate', 'transform', 'load', 'verify'],
            'ml_pipeline': ['extract', 'preprocess', 'train', 'evaluate', 'deploy'],
            'backup': ['validate', 'backup', 'verify', 'notify'],
            'reporting': ['extract', 'analyze', 'report', 'notify']
        }
        
        # Detect workflow pattern
        detected_pattern = None
        for pattern_name, pattern_steps in workflow_patterns.items():
            if any(step in description_lower for step in pattern_steps):
                detected_pattern = pattern_steps
                break
        
        if detected_pattern:
            # Sort tasks based on detected pattern
            ordered_tasks = []
            remaining_tasks = tasks.copy()
            
            for step in detected_pattern:
                # Find task that matches this step
                for task in remaining_tasks:
                    task_id = task['task_id'].lower()
                    description = task.get('description', '').lower()
                    if step in task_id or step in description:
                        ordered_tasks.append(task)
                        remaining_tasks.remove(task)
                        break
            
            # Add any remaining tasks at the end
            ordered_tasks.extend(remaining_tasks)
            tasks = ordered_tasks
        
        # Set up dependencies - each task depends on the previous one
        for i in range(1, len(tasks)):
            tasks[i]['dependencies'] = [tasks[i-1]['task_id']]
        
        return tasks

    def _parse_tasks_from_description(self, description: str) -> List[Dict[str, Any]]:
        """Parse specific tasks from natural language description."""
        tasks = []
        description_lower = description.lower()

        # Pattern 1: "task X does Y" or "task X prints Y"
        task_patterns = [
            r'task\s+(\w+)\s+(?:prints?|does|runs?|executes?)\s+(.+?)(?:,|\sand\s|$)',
            r'task\s+(\d+|[a-z]+)\s+(?:prints?|does|runs?|executes?)\s+(.+?)(?:,|\sand\s|$)'
        ]

        for pattern in task_patterns:
            matches = re.findall(pattern, description_lower, re.IGNORECASE)
            for match in matches:
                task_num, action = match
                task_id = f"task_{task_num.lower()}"
                task = self._create_task_from_action_enhanced(task_id, action.strip())
                tasks.append(task)

        # Pattern 2: Simple task lists like "task one, task two"
        if not tasks:
            simple_task_pattern = r'task\s+(\w+)(?:\s*,\s*|\s*$)'
            simple_matches = re.findall(simple_task_pattern, description_lower, re.IGNORECASE)

            for i, task_name in enumerate(simple_matches):
                task_id = f"task_{task_name.lower()}"
                task = {
                    "task_id": task_id,
                    "task_type": "python",
                    "python_code": f"print('Executing {task_name}')\n# Add your logic here\nreturn {{'status': '{task_name}_completed'}}",
                    "dependencies": [tasks[-1]["task_id"]] if tasks else [],
                    "provide_context": False
                }
                tasks.append(task)

        return tasks

    def _create_task_from_action(self, task_id: str, action: str) -> Dict[str, Any]:
        """Create a task based on the parsed action."""
        action_lower = action.lower()

        # Determine task type and code based on action
        if 'print' in action_lower:
            # Extract what to print
            print_match = re.search(r'print\s+(.+)$', action, re.IGNORECASE)
            if print_match:
                print_text = print_match.group(1).strip()
                code = f"print('{print_text}')\nreturn {{'status': 'printed'}}"
                task_type = "python"
            else:
                code = f"print('Executing {task_id}')\nreturn {{'status': 'completed'}}"
                task_type = "python"
        elif 'run' in action_lower or 'execute' in action_lower:
            # Could be a bash command
            code = f"echo 'Running {action}'\n# Add your command here"
            task_type = "bash"
        elif 'send' in action_lower and ('email' in action_lower or 'notification' in action_lower):
            code = f"echo 'Sending notification: {action}'\ncurl -X POST your-webhook-url"
            task_type = "bash"
        else:
            # Default to Python
            code = f"print('Executing: {action}')\n# Add your logic here\nreturn {{'status': 'completed'}}"
            task_type = "python"

        return {
            "task_id": task_id,
            "task_type": task_type,
            "python_code": code if task_type == "python" else None,
            "bash_command": code if task_type == "bash" else None,
            "dependencies": [],  # Will be set by caller
            "provide_context": False
        }

    def _create_generic_tasks(self, description: str, description_lower: str) -> List[Dict[str, Any]]:
        """Create generic tasks based on keyword patterns (fallback)."""
        tasks = []

        if 'pipeline' in description_lower or 'data' in description_lower:
            tasks.extend([
                {
                    "task_id": "extract_data",
                    "task_type": "python",
                    "python_code": "print('Extracting data...')\n# Add your data extraction logic here\nreturn {'status': 'extracted'}",
                    "dependencies": [],
                    "provide_context": False
                },
                {
                    "task_id": "process_data",
                    "task_type": "python",
                    "python_code": "print('Processing data...')\n# Add your data processing logic here\nreturn {'status': 'processed'}",
                    "dependencies": ["extract_data"],
                    "provide_context": False
                }
            ])

        if 'report' in description_lower or 'analytics' in description_lower:
            tasks.append({
                "task_id": "generate_report",
                "task_type": "python",
                "python_code": "print('Generating report...')\n# Add your reporting logic here\nreturn {'status': 'report_generated'}",
                "dependencies": ["process_data"] if tasks else [],
                "provide_context": False
            })

        if 'notification' in description_lower or 'alert' in description_lower:
            tasks.append({
                "task_id": "send_notification",
                "task_type": "bash",
                "bash_command": "echo 'Sending notification...' && curl -X POST your-webhook-url",
                "dependencies": [tasks[-1]["task_id"]] if tasks else [],
                "provide_context": False
            })

        # If no specific tasks identified, create a generic workflow
        if not tasks:
            tasks = [
                {
                    "task_id": "execute_workflow",
                    "task_type": "python",
                    "python_code": f"print('Executing: {description}')\n# Add your workflow logic here\nreturn {{'status': 'completed'}}",
                    "dependencies": [],
                    "provide_context": False
                }
            ]

        return tasks

    def _ensure_complete_dag_spec(self, dag_spec: Dict[str, Any], original_description: str, complexity: str) -> Dict[str, Any]:
        """Ensure a DAG spec has all required sections, adding them if missing."""
        dag_id = dag_spec.get('dag_id', 'fallback_dag')

        # Ensure supabase_paths section
        if 'supabase_paths' not in dag_spec:
            dag_spec['supabase_paths'] = {
                "main_file": f"projects/{{{{project_id}}}}/dags/{dag_id}/{dag_id}.py",
                "config_file": f"projects/{{{{project_id}}}}/dags/{dag_id}/config.json",
                "readme_file": f"projects/{{{{project_id}}}}/dags/{dag_id}/README.md",
                "utils_init": f"projects/{{{{project_id}}}}/dags/{dag_id}/utils/__init__.py",
                "utils_helpers": f"projects/{{{{project_id}}}}/dags/{dag_id}/utils/helpers.py"
            }

        # Ensure airflow_metadata section
        if 'airflow_metadata' not in dag_spec:
            dag_spec['airflow_metadata'] = {
                "dag_file": f"{dag_id}.py",
                "config_file": "config.json",
                "start_date": "2024-01-01",
                "concurrency": 1,
                "max_active_runs": 1,
                "dagrun_timeout_hours": 2,
                "catchup": False
            }

        # Ensure directory_structure is complete
        if 'directory_structure' not in dag_spec:
            dag_spec['directory_structure'] = {
                "base_directory": f"dags/{dag_id}/",
                "main_file": f"dags/{dag_id}/{dag_id}.py",
                "config_file": f"dags/{dag_id}/config.json",
                "readme_file": f"dags/{dag_id}/README.md",
                "utils_directory": f"dags/{dag_id}/utils/",
                "utils_init": f"dags/{dag_id}/utils/__init__.py",
                "utils_helpers": f"dags/{dag_id}/utils/helpers.py"
            }

        # Ensure validation_checks
        if 'validation_checks' not in dag_spec:
            dag_spec['validation_checks'] = {
                "reasonable_task_count": True,
                "logical_dependencies": True,
                "proper_naming": True,
                "complete_structure": True,
                "no_circular_dependencies": True,
                "airflow_compatible": True,
                "supabase_ready": True
            }

        # Update description if it's too generic
        if dag_spec.get('description') == original_description:
            dag_spec['description'] = f"Auto-generated DAG for: {original_description}"

        # Ensure owner is set
        if not dag_spec.get('owner') or dag_spec['owner'] == 'user':
            dag_spec['owner'] = 'data-team'

        # Ensure tags are meaningful
        if not dag_spec.get('tags'):
            dag_spec['tags'] = self._extract_tags(original_description.lower())
            if not dag_spec['tags']:
                dag_spec['tags'] = ['auto-generated']

        return dag_spec

    def _create_smart_fallback_tasks(self, description: str, description_lower: str) -> List[Dict[str, Any]]:
        """Create smart fallback tasks based on description analysis."""
        tasks = []

        # Analyze description for common patterns
        if any(word in description_lower for word in ['health', 'check', 'monitor', 'status']):
            # Health check pattern
            tasks.extend([
                {
                    "task_id": "check_system_resources",
                    "task_type": "bash",
                    "bash_command": "echo 'Checking CPU, Memory, and Disk usage...' && df -h && free -h && uptime",
                    "description": "Check basic system resource usage",
                    "dependencies": [],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 10
                },
                {
                    "task_id": "validate_thresholds",
                    "task_type": "python",
                    "python_code": "# Check if system metrics exceed thresholds\\nfrom airflow.models import Variable\\n\\n# Get thresholds from Airflow variables\\ncpu_threshold = float(Variable.get('cpu_threshold', '80'))\\nmemory_threshold = float(Variable.get('memory_threshold', '85'))\\ndisk_threshold = float(Variable.get('disk_threshold', '90'))\\n\\nprint(f'Checking thresholds: CPU={cpu_threshold}%, Memory={memory_threshold}%, Disk={disk_threshold}%')\\n\\n# In a real scenario, you'd check actual metrics\\n# For now, simulate healthy system\\nmetrics = {\\n    'cpu_usage': 45.2,\\n    'memory_usage': 62.8,\\n    'disk_usage': 73.1\\n}\\n\\n# Check thresholds\\nalerts = []\\nif metrics['cpu_usage'] > cpu_threshold:\\n    alerts.append(f'CPU usage {metrics[\"cpu_usage\"]}% exceeds threshold {cpu_threshold}%')\\nif metrics['memory_usage'] > memory_threshold:\\n    alerts.append(f'Memory usage {metrics[\"memory_usage\"]}% exceeds threshold {memory_threshold}%')\\nif metrics['disk_usage'] > disk_threshold:\\n    alerts.append(f'Disk usage {metrics[\"disk_usage\"]}% exceeds threshold {disk_threshold}%')\\n\\nprint(f'System metrics: {metrics}')\\nif alerts:\\n    print(f'Alerts: {alerts}')\\n    return {'status': 'alerts_found', 'alerts': alerts, 'metrics': metrics}\\nelse:\\n    print('All metrics within thresholds')\\n    return {'status': 'healthy', 'metrics': metrics}",
                    "description": "Validate system metrics against configured thresholds",
                    "dependencies": ["check_system_resources"],
                    "provide_context": True,
                    "retries": 1,
                    "retry_delay_minutes": 2,
                    "execution_timeout_minutes": 15
                }
            ])

        elif any(word in description_lower for word in ['data', 'pipeline', 'process', 'etl']):
            # Data pipeline pattern
            tasks.extend([
                {
                    "task_id": "extract_data",
                    "task_type": "python",
                    "python_code": "# Extract data from source\\nprint('Extracting data from source...')\\n# Add your data extraction logic here\\ndata = {'records': 1000, 'source': 'database'}\\nprint(f'Extracted {{data[\"records\"]}} records from {{data[\"source\"]}}')\\nreturn {'status': 'extracted', 'data': data}",
                    "description": "Extract data from configured source",
                    "dependencies": [],
                    "provide_context": False,
                    "retries": 3,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 30
                },
                {
                    "task_id": "transform_data",
                    "task_type": "python",
                    "python_code": "# Transform extracted data\\nprint('Transforming data...')\\n# Add your data transformation logic here\\n# In a real scenario, you'd process the data from extract step\\ntransformed_data = {'processed_records': 950, 'quality_score': 0.95}\\nprint(f'Transformed {{transformed_data[\"processed_records\"]}} records')\\nreturn {'status': 'transformed', 'data': transformed_data}",
                    "description": "Transform and clean extracted data",
                    "dependencies": ["extract_data"],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 3,
                    "execution_timeout_minutes": 25
                }
            ])

        elif any(word in description_lower for word in ['report', 'analytics', 'dashboard']):
            # Reporting pattern
            tasks.extend([
                {
                    "task_id": "generate_report",
                    "task_type": "python",
                    "python_code": "# Generate analytics report\\nprint('Generating analytics report...')\\n# Add your report generation logic here\\nreport = {\\n    'total_records': 15000,\\n    'date_range': '2024-01-01 to 2024-01-31',\\n    'metrics': {\\n        'conversion_rate': 3.45,\\n        'avg_session_duration': 245\\n    }\\n}\\nprint(f'Report generated for {{report[\"total_records\"]}} records')\\nreturn {'status': 'report_generated', 'report': report}",
                    "description": "Generate analytics report with key metrics",
                    "dependencies": [],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 20
                }
            ])

        elif any(word in description_lower for word in ['healthcare', 'hipaa', 'medical', 'clinical', 'patient']):
            # Healthcare data pipeline pattern
            tasks.extend([
                {
                    "task_id": "extract_patient_data",
                    "task_type": "python",
                    "python_code": "# Extract patient data from EHR systems\\nprint('Extracting patient data from EHR systems...')\\n# Add HIPAA-compliant data extraction logic here\\nfrom airflow.models import Variable\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    ehr_api_key = Variable.get('ehr_api_key')\\n    logger.info('Extracting patient data with proper authentication')\\n    # Add data extraction logic here\\n    patient_data = {'records': 1500, 'sources': ['ehr_system_1', 'ehr_system_2']}\\n    logger.info(f'Extracted {{patient_data[\"records\"]}} records from {{len(patient_data[\"sources\"])}} sources')\\n    return {'status': 'data_extracted', 'patient_data': patient_data}\\nexcept Exception as e:\\n    logger.error(f'Data extraction failed: {{str(e)}}')\\n    raise",
                    "description": "Extract patient data from multiple EHR systems with HIPAA compliance",
                    "dependencies": [],
                    "provide_context": True,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 20
                },
                {
                    "task_id": "standardize_normalize_data",
                    "task_type": "python",
                    "python_code": "# Standardize and normalize healthcare data\\nprint('Standardizing and normalizing healthcare data...')\\n# Add data standardization logic here\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    # Add data transformation logic here\\n    standardized_data = {'total_records': 1500, 'standardized_fields': ['patient_id', 'diagnosis_codes', 'medications']}\\n    logger.info(f'Standardized {{standardized_data[\"total_records\"]}} records')\\n    return {'status': 'data_standardized', 'standardized_data': standardized_data}\\nexcept Exception as e:\\n    logger.error(f'Data standardization failed: {{str(e)}}')\\n    raise",
                    "description": "Standardize and normalize patient data to internal schema",
                    "dependencies": ["extract_patient_data"],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 15
                },
                {
                    "task_id": "apply_privacy_security",
                    "task_type": "python",
                    "python_code": "# Apply privacy and security controls\\nprint('Applying HIPAA privacy and security controls...')\\n# Add privacy masking and encryption logic here\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    # Add PHI masking logic here\\n    security_result = {'records_processed': 1500, 'phi_masked': 85, 'encrypted_fields': ['ssn', 'address', 'phone']}\\n    logger.info(f'Applied privacy controls to {{security_result[\"records_processed\"]}} records')\\n    return {'status': 'privacy_applied', 'security_result': security_result}\\nexcept Exception as e:\\n    logger.error(f'Privacy controls failed: {{str(e)}}')\\n    raise",
                    "description": "Apply HIPAA-compliant privacy controls and data masking",
                    "dependencies": ["standardize_normalize_data"],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 15
                },
                {
                    "task_id": "audit_log_events",
                    "task_type": "python",
                    "python_code": "# Implement comprehensive audit logging\\nprint('Implementing comprehensive audit logging...')\\n# Add audit logging logic here\\nimport logging\\nfrom datetime import datetime\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    # Add audit logging logic here\\n    audit_entry = {\\n        'timestamp': datetime.now().isoformat(),\\n        'action': 'data_processing',\\n        'user': 'system',\\n        'records_processed': 1500,\\n        'compliance_status': 'HIPAA_compliant'\\n    }\\n    logger.info(f'Audit log created: {{audit_entry[\"action\"]}} at {{audit_entry[\"timestamp\"]}}')\\n    return {'status': 'audit_logged', 'audit_entry': audit_entry}\\nexcept Exception as e:\\n    logger.error(f'Audit logging failed: {{str(e)}}')\\n    raise",
                    "description": "Implement comprehensive audit logging for HIPAA compliance",
                    "dependencies": ["apply_privacy_security"],
                    "provide_context": False,
                    "retries": 1,
                    "retry_delay_minutes": 2,
                    "execution_timeout_minutes": 10
                }
            ])

        elif any(word in description_lower for word in ['weather', 'api', 'synchronization', 'sync', 'fetch']):
            # Weather API data synchronization pattern
            tasks.extend([
                {
                    "task_id": "authenticate_apis",
                    "task_type": "python",
                    "python_code": "# Authenticate with weather APIs\\nprint('Authenticating with weather APIs...')\\n# Add API authentication logic here\\nfrom airflow.models import Variable\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    api_keys = {\\n        'openweather_key': Variable.get('openweather_api_key'),\\n        'weatherstack_key': Variable.get('weatherstack_api_key'),\\n        'accuweather_key': Variable.get('accuweather_api_key')\\n    }\\n    logger.info(f'Authenticated with {{len([k for k in api_keys.values() if k])}} weather APIs')\\n    return {'status': 'authenticated', 'api_keys': {k: '***masked***' for k in api_keys.keys()}}\\nexcept Exception as e:\\n    logger.error(f'API authentication failed: {{str(e)}}')\\n    raise",
                    "description": "Authenticate with multiple weather APIs",
                    "dependencies": [],
                    "provide_context": True,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 10
                },
                {
                    "task_id": "fetch_weather_data",
                    "task_type": "python",
                    "python_code": "# Fetch weather data from APIs\\nprint('Fetching weather data from APIs...')\\n# Add data fetching logic here\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    # Add weather data fetching logic here\\n    weather_data = {\\n        'locations': ['New York', 'London', 'Tokyo'],\\n        'data_points': 150,\\n        'apis_used': ['openweather', 'weatherstack'],\\n        'timestamp': '2024-01-15T10:00:00Z'\\n    }\\n    logger.info(f'Fetched weather data for {{len(weather_data[\"locations\"])}} locations from {{len(weather_data[\"apis_used\"])}} APIs')\\n    return {'status': 'data_fetched', 'weather_data': weather_data}\\nexcept Exception as e:\\n    logger.error(f'Data fetching failed: {{str(e)}}')\\n    raise",
                    "description": "Fetch weather data from multiple free APIs",
                    "dependencies": ["authenticate_apis"],
                    "provide_context": False,
                    "retries": 3,
                    "retry_delay_minutes": 10,
                    "execution_timeout_minutes": 20
                },
                {
                    "task_id": "transform_data_schema",
                    "task_type": "python",
                    "python_code": "# Transform data to internal schema\\nprint('Transforming weather data to internal schema...')\\n# Add data transformation logic here\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    # Add schema transformation logic here\\n    transformed_data = {\\n        'total_records': 150,\\n        'transformed_fields': ['temperature_c', 'humidity_pct', 'wind_speed_kmh'],\\n        'schema_version': 'internal_v2.0'\\n    }\\n    logger.info(f'Transformed {{transformed_data[\"total_records\"]}} records to {{transformed_data[\"schema_version\"]}} schema')\\n    return {'status': 'data_transformed', 'transformed_data': transformed_data}\\nexcept Exception as e:\\n    logger.error(f'Data transformation failed: {{str(e)}}')\\n    raise",
                    "description": "Transform weather data to match internal schema",
                    "dependencies": ["fetch_weather_data"],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 15
                },
                {
                    "task_id": "validate_data_quality",
                    "task_type": "python",
                    "python_code": "# Validate data quality\\nprint('Validating weather data quality...')\\n# Add data validation logic here\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    # Add data validation logic here\\n    validation_result = {\\n        'total_records': 150,\\n        'valid_records': 145,\\n        'invalid_records': 5,\\n        'validation_checks': ['temperature_range', 'location_format', 'timestamp_valid']\\n    }\\n    logger.info(f'Validated {{validation_result[\"valid_records\"]}}/{{validation_result[\"total_records\"]}} records successfully')\\n    return {'status': 'data_validated', 'validation_result': validation_result}\\nexcept Exception as e:\\n    logger.error(f'Data validation failed: {{str(e)}}')\\n    raise",
                    "description": "Perform data validation and quality checks",
                    "dependencies": ["transform_data_schema"],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 10
                },
                {
                    "task_id": "handle_duplicates",
                    "task_type": "python",
                    "python_code": "# Handle duplicate records\\nprint('Handling duplicate weather records...')\\n# Add duplicate handling logic here\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    # Add deduplication logic here\\n    dedup_result = {\\n        'original_records': 150,\\n        'duplicates_removed': 12,\\n        'final_records': 138,\\n        'dedup_method': 'timestamp_location_hash'\\n    }\\n    logger.info(f'Removed {{dedup_result[\"duplicates_removed\"]}} duplicate records, keeping {{dedup_result[\"final_records\"]}}')\\n    return {'status': 'duplicates_handled', 'dedup_result': dedup_result}\\nexcept Exception as e:\\n    logger.error(f'Duplicate handling failed: {{str(e)}}')\\n    raise",
                    "description": "Handle duplicate records in weather data",
                    "dependencies": ["validate_data_quality"],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 10
                },
                {
                    "task_id": "load_to_staging",
                    "task_type": "python",
                    "python_code": "# Load data to staging tables\\nprint('Loading weather data to staging tables...')\\n# Add staging table loading logic here\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    # Add database loading logic here\\n    load_result = {\\n        'records_loaded': 138,\\n        'staging_table': 'weather_staging',\\n        'load_timestamp': '2024-01-15T10:30:00Z',\\n        'batch_size': 50\\n    }\\n    logger.info(f'Loaded {{load_result[\"records_loaded\"]}} records to {{load_result[\"staging_table\"]}} table')\\n    return {'status': 'data_loaded', 'load_result': load_result}\\nexcept Exception as e:\\n    logger.error(f'Data loading failed: {{str(e)}}')\\n    raise",
                    "description": "Load processed data into staging tables",
                    "dependencies": ["handle_duplicates"],
                    "provide_context": False,
                    "retries": 3,
                    "retry_delay_minutes": 10,
                    "execution_timeout_minutes": 20
                },
                {
                    "task_id": "trigger_downstream",
                    "task_type": "python",
                    "python_code": "# Trigger downstream processing workflows\\nprint('Triggering downstream processing workflows...')\\n# Add workflow triggering logic here\\nimport logging\\n\\nlogger = logging.getLogger(__name__)\\ntry:\\n    # Add downstream workflow triggering logic here\\n    trigger_result = {\\n        'workflows_triggered': ['analytics_pipeline', 'forecast_model'],\\n        'trigger_timestamp': '2024-01-15T10:35:00Z',\\n        'data_ready': True\\n    }\\n    logger.info(f'Triggered {{len(trigger_result[\"workflows_triggered\"])}} downstream workflows')\\n    return {'status': 'workflows_triggered', 'trigger_result': trigger_result}\\nexcept Exception as e:\\n    logger.error(f'Workflow triggering failed: {{str(e)}}')\\n    raise",
                    "description": "Trigger downstream processing workflows",
                    "dependencies": ["load_to_staging"],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 10
                }
            ])

        elif any(word in description_lower for word in ['saas', 'multi-tenant', 'platform', 'tenant']):
            # SaaS/Multi-tenant platform management pattern
            tasks.extend([
                {
                    "task_id": "check_tenant_health",
                    "task_type": "python",
                    "python_code": "# Check health of all tenants\\nprint('Checking tenant health...')\\n# Add tenant health check logic here\\ntenants = [{'id': 'tenant_1', 'status': 'healthy'}, {'id': 'tenant_2', 'status': 'warning'}]\\nprint(f'Checked {{len(tenants)}} tenants')\\nreturn {'status': 'health_checked', 'tenants': tenants}",
                    "description": "Check health status of all tenants",
                    "dependencies": [],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 15
                },
                {
                    "task_id": "manage_resources",
                    "task_type": "python",
                    "python_code": "# Manage resource allocation\\nprint('Managing resource allocation...')\\n# Add resource management logic here\\nresources = {'cpu': 85, 'memory': 72, 'storage': 60}\\nprint(f'Resources managed: CPU {{resources[\"cpu\"]}}%, Memory {{resources[\"memory\"]}}%')\\nreturn {'status': 'resources_managed', 'resources': resources}",
                    "description": "Manage resource allocation across tenants",
                    "dependencies": ["check_tenant_health"],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 20
                },
                {
                    "task_id": "backup_tenant_data",
                    "task_type": "python",
                    "python_code": "# Backup tenant data\\nprint('Backing up tenant data...')\\n# Add backup logic here\\nbackup_result = {'backups_completed': 5, 'total_size_gb': 2.3}\\nprint(f'Backed up {{backup_result[\"backups_completed\"]}} tenants, {{backup_result[\"total_size_gb\"]}}GB')\\nreturn {'status': 'backup_completed', 'backup': backup_result}",
                    "description": "Create backups of tenant data",
                    "dependencies": ["manage_resources"],
                    "provide_context": False,
                    "retries": 3,
                    "retry_delay_minutes": 10,
                    "execution_timeout_minutes": 30
                }
            ])

        # Always add a notification task unless it's specifically excluded
        if not any(word in description_lower for word in ['no notification', 'skip alert', 'quiet']):
            tasks.append({
                "task_id": "send_notification",
                "task_type": "bash",
                "bash_command": "echo 'Workflow completed successfully - sending notification...' && echo '✅ DAG execution completed at $(date)'",
                "description": "Send completion notification",
                "dependencies": [tasks[-1]["task_id"]] if tasks else [],
                "provide_context": False,
                "retries": 1,
                "retry_delay_minutes": 2,
                "execution_timeout_minutes": 5
            })

        # If still no tasks, create a generic workflow
        if not tasks:
            # Check if this is a very complex workflow that needs breakdown
            complex_indicators = ['saas', 'platform', 'multi-tenant', 'enterprise', 'scalable', 'distributed']
            is_very_complex = any(word in description_lower for word in complex_indicators) and len(description.split()) > 20

            if is_very_complex:
                # Create multiple tasks for complex workflows
                tasks = [
                    {
                        "task_id": "analyze_requirements",
                        "task_type": "python",
                        "python_code": f"# Analyze complex workflow requirements\\nprint('Analyzing complex workflow: {description[:50]}...')\\n# This is a complex workflow that needs detailed implementation\\nanalysis = {{'complexity': 'high', 'components': ['multiple systems', 'coordination needed']}}\\nprint('Analysis complete - manual implementation recommended')\\nreturn analysis",
                        "description": "Analyze complex workflow requirements",
                        "dependencies": [],
                        "provide_context": False,
                        "retries": 1,
                        "retry_delay_minutes": 2,
                        "execution_timeout_minutes": 15
                    },
                    {
                        "task_id": "create_implementation_plan",
                        "task_type": "python",
                        "python_code": f"# Create detailed implementation plan\\nprint('Creating implementation plan for: {description[:50]}...')\\n# Break down complex workflow into manageable components\\nplan = {{'phases': ['design', 'development', 'testing', 'deployment'], 'estimated_effort': 'high'}}\\nprint('Implementation plan created')\\nreturn plan",
                        "description": "Create detailed implementation plan",
                        "dependencies": ["analyze_requirements"],
                        "provide_context": False,
                        "retries": 1,
                        "retry_delay_minutes": 2,
                        "execution_timeout_minutes": 10
                    },
                    {
                        "task_id": "setup_basic_structure",
                        "task_type": "python",
                        "python_code": f"# Set up basic workflow structure\\nprint('Setting up basic structure for complex workflow...')\\n# Initialize core components and framework\\nstructure = {{'status': 'initialized', 'components_ready': ['logging', 'error_handling', 'monitoring']}}\\nprint('Basic structure initialized')\\nreturn structure",
                        "description": "Set up basic workflow structure",
                        "dependencies": ["create_implementation_plan"],
                        "provide_context": False,
                        "retries": 2,
                        "retry_delay_minutes": 5,
                        "execution_timeout_minutes": 20
                    }
                ]
            else:
                # Simple generic workflow
                tasks = [
                    {
                        "task_id": "execute_workflow",
                        "task_type": "python",
                        "python_code": f"# Generic workflow execution\\nprint('Executing workflow: {description}')\\n# Add your custom logic here\\nresult = {{'status': 'completed', 'description': '{description}'}}\\nprint(f'Workflow execution result: {{result}}')\\nreturn result",
                        "description": "Execute the main workflow logic",
                        "dependencies": [],
                        "provide_context": False,
                        "retries": 2,
                        "retry_delay_minutes": 5,
                        "execution_timeout_minutes": 30
                    }
                ]

        return tasks

    def _clean_llm_response(self, response: str) -> str:
        """Clean and normalize LLM response to improve JSON parsing."""
        if not response:
            return response

        # Remove markdown code blocks if present
        if response.startswith('```json'):
            response = response[7:]
        if response.startswith('```'):
            response = response[3:]
        if response.endswith('```'):
            response = response[:-3]

        # Remove any leading/trailing whitespace
        response = response.strip()

        # Handle common JSON formatting issues
        # Fix trailing commas before closing braces/brackets
        response = re.sub(r',(\s*[}\]])', r'\1', response)

        # Fix missing commas between key-value pairs (simple cases)
        response = re.sub(r'}(\s*")', r'},\1', response)
        response = re.sub(r'](\s*")', r'],\1', response)

        # Fix unterminated strings by closing them
        # Look for strings that start with quotes but don't end with quotes
        lines = response.split('\n')
        fixed_lines = []
        in_string = False
        string_char = None

        for line in lines:
            # Check for unterminated strings in this line
            if '"' in line or "'" in line:
                # Simple fix: if we find a quote at the end of a line that looks like it should be closed
                if line.rstrip().endswith(':') and not line.strip().endswith('",') and not line.strip().endswith('"'):
                    # This looks like a key that should have a string value
                    line = line.rstrip() + ' ""'
                elif line.rstrip().endswith(',') and not (line.strip().endswith('",') or line.strip().endswith("',")):
                    # This looks like a value that should be a string
                    line = re.sub(r',(\s*)$', r', ""\1', line)

            fixed_lines.append(line)

        response = '\n'.join(fixed_lines)

        # Remove any remaining non-JSON text at the beginning
        json_start = response.find('{')
        if json_start > 0:
            response = response[json_start:]

        # Remove any remaining non-JSON text at the end
        json_end = response.rfind('}')
        if json_end >= 0 and json_end < len(response) - 1:
            response = response[:json_end + 1]

        # Final cleanup: ensure we have valid JSON structure
        response = response.strip()
        if not response.startswith('{'):
            response = '{' + response
        if not response.endswith('}'):
            response = response + '}'

        return response

    def _parse_json_robustly(self, response: str) -> Optional[Dict[str, Any]]:
        """Parse JSON with multiple fallback strategies for malformed responses."""
        # Strategy 1: Try direct parsing
        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            print(f"Direct JSON parsing failed: {e}")

        # Strategy 2: Try cleaning the response first
        try:
            cleaned = self._clean_llm_response(response)
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            print(f"Cleaned JSON parsing failed: {e}")

        # Strategy 3: Try to extract and fix partial JSON
        try:
            return self._parse_partial_json(response)
        except Exception as e:
            print(f"Partial JSON parsing failed: {e}")

        # Strategy 3.5: Try to fix common JSON issues in large responses
        try:
            return self._fix_common_json_issues(response)
        except Exception as e:
            print(f"JSON fixing failed: {e}")

        # Strategy 4: Last resort - try to create a minimal valid spec from any extracted data
        try:
            return self._create_minimal_spec_from_response(response)
        except Exception as e:
            print(f"Minimal spec creation failed: {e}")

        return None

    def _parse_partial_json(self, response: str) -> Optional[Dict[str, Any]]:
        """Try to parse partial or malformed JSON by extracting valid parts."""
        # Look for key components that we can extract
        dag_name_match = re.search(r'"dag_name"\s*:\s*"([^"]*)"', response)
        dag_id_match = re.search(r'"dag_id"\s*:\s*"([^"]*)"', response)
        description_match = re.search(r'"description"\s*:\s*"([^"]*)"', response, re.DOTALL)

        if dag_name_match and dag_id_match:
            # We have the basic required fields, create a minimal spec
            minimal_spec = {
                "dag_name": dag_name_match.group(1),
                "dag_id": dag_id_match.group(1),
                "description": description_match.group(1) if description_match else f"Auto-generated DAG for {dag_name_match.group(1)}",
                "schedule_interval": "@daily",
                "owner": "data-team",
                "tags": ["auto-generated"],
                "complexity": "simple",
                "estimated_tasks": 1,
                "tasks": [
                    {
                        "task_id": "placeholder_task",
                        "task_type": "python",
                        "description": "Placeholder task - please review and customize",
                        "python_code": "print('Placeholder task - customize as needed')\nreturn {'status': 'completed'}",
                        "dependencies": [],
                        "provide_context": False,
                        "retries": 1,
                        "retry_delay_minutes": 5,
                        "execution_timeout_minutes": 30
                    }
                ],
                "directory_structure": {
                    "base_directory": f"dags/{dag_id_match.group(1)}/",
                    "main_file": f"dags/{dag_id_match.group(1)}/{dag_id_match.group(1)}.py",
                    "config_file": f"dags/{dag_id_match.group(1)}/config.json",
                    "readme_file": f"dags/{dag_id_match.group(1)}/README.md",
                    "utils_directory": f"dags/{dag_id_match.group(1)}/utils/",
                    "utils_init": f"dags/{dag_id_match.group(1)}/utils/__init__.py",
                    "utils_helpers": f"dags/{dag_id_match.group(1)}/utils/helpers.py"
                },
                "supabase_paths": {
                    "main_file": f"projects/{{{{project_id}}}}/dags/{dag_id_match.group(1)}/{dag_id_match.group(1)}.py",
                    "config_file": f"projects/{{{{project_id}}}}/dags/{dag_id_match.group(1)}/config.json",
                    "readme_file": f"projects/{{{{project_id}}}}/dags/{dag_id_match.group(1)}/README.md",
                    "utils_init": f"projects/{{{{project_id}}}}/dags/{dag_id_match.group(1)}/utils/__init__.py",
                    "utils_helpers": f"projects/{{{{project_id}}}}/dags/{dag_id_match.group(1)}/utils/helpers.py"
                },
                "validation_checks": {
                    "reasonable_task_count": True,
                    "logical_dependencies": True,
                    "proper_naming": True,
                    "complete_structure": True,
                    "no_circular_dependencies": True,
                    "airflow_compatible": True,
                    "supabase_ready": True
                },
                "airflow_metadata": {
                    "dag_file": f"{dag_id_match.group(1)}.py",
                    "config_file": "config.json",
                    "start_date": "2024-01-01",
                    "concurrency": 1,
                    "max_active_runs": 1,
                    "dagrun_timeout_hours": 2,
                    "catchup": False
                }
            }
            print(f"✅ Created minimal spec from partial JSON: {dag_name_match.group(1)}")
            return minimal_spec

        return None

    def _create_minimal_spec_from_response(self, response: str) -> Dict[str, Any]:
        """Create a minimal but valid DAG spec when all else fails."""
        # Extract any identifiable information from the response
        lines = response.split('\n')
        dag_name = "Database Maintenance Routine"  # Default fallback
        dag_id = "database_maintenance_fallback"

        # Try to find any meaningful identifiers
        for line in lines[:10]:  # Check first 10 lines
            if '"dag_name"' in line:
                name_match = re.search(r'"dag_name"\s*:\s*"([^"]*)"', line)
                if name_match:
                    dag_name = name_match.group(1)
                    dag_id = re.sub(r'[^a-z0-9_]', '_', dag_name.lower())[:50]
                    break

        # Create a minimal working spec
        minimal_spec = {
            "dag_name": dag_name,
            "dag_id": dag_id,
            "description": f"Auto-generated fallback DAG for: {dag_name}. Original response had JSON parsing issues.",
            "schedule_interval": "@daily",
            "owner": "data-team",
            "tags": ["auto-generated", "fallback"],
            "complexity": "simple",
            "estimated_tasks": 1,
            "tasks": [
                {
                    "task_id": "maintenance_task",
                    "task_type": "python",
                    "description": "Basic maintenance task - please customize as needed",
                    "python_code": "import logging\n\nlogger = logging.getLogger(__name__)\n\ntry:\n    logger.info('Running maintenance task')\n    # Add your maintenance logic here\n    result = {'status': 'completed', 'message': 'Maintenance task executed'}\n    logger.info('Maintenance completed successfully')\n    return result\nexcept Exception as e:\n    logger.error(f'Maintenance failed: {str(e)}')\n    raise",
                    "dependencies": [],
                    "provide_context": False,
                    "retries": 2,
                    "retry_delay_minutes": 5,
                    "execution_timeout_minutes": 30
                }
            ],
            "directory_structure": {
                "base_directory": f"dags/{dag_id}/",
                "main_file": f"dags/{dag_id}/{dag_id}.py",
                "config_file": f"dags/{dag_id}/config.json",
                "readme_file": f"dags/{dag_id}/README.md",
                "utils_directory": f"dags/{dag_id}/utils/",
                "utils_init": f"dags/{dag_id}/utils/__init__.py",
                "utils_helpers": f"dags/{dag_id}/utils/helpers.py"
            },
            "supabase_paths": {
                "main_file": f"projects/{{{{project_id}}}}/dags/{dag_id}/{dag_id}.py",
                "config_file": f"projects/{{{{project_id}}}}/dags/{dag_id}/config.json",
                "readme_file": f"projects/{{{{project_id}}}}/dags/{dag_id}/README.md",
                "utils_init": f"projects/{{{{project_id}}}}/dags/{dag_id}/utils/__init__.py",
                "utils_helpers": f"projects/{{{{project_id}}}}/dags/{dag_id}/utils/helpers.py"
            },
            "validation_checks": {
                "reasonable_task_count": True,
                "logical_dependencies": True,
                "proper_naming": True,
                "complete_structure": True,
                "no_circular_dependencies": True,
                "airflow_compatible": True,
                "supabase_ready": True
            },
            "airflow_metadata": {
                "dag_file": f"{dag_id}.py",
                "config_file": "config.json",
                "start_date": "2024-01-01",
                "concurrency": 1,
                "max_active_runs": 1,
                "dagrun_timeout_hours": 2,
                "catchup": False
            }
        }

        print(f"✅ Created fallback spec due to JSON parsing failure: {dag_name}")
        return minimal_spec

    def _has_meaningful_tasks(self, spec: Dict[str, Any]) -> bool:
        """Check if the DAG spec has meaningful tasks instead of placeholders."""
        tasks = spec.get('tasks', [])
        if not tasks:
            return False
        
        # Check for placeholder indicators - be more specific to avoid false positives
        strict_placeholder_indicators = [
            'placeholder task', 'customize as needed', 'add your logic here',
            'placeholder - please review', 'modify this', 'example only'
        ]
        
        meaningful_count = 0
        
        for task in tasks:
            task_id = task.get('task_id', '').lower()
            description = task.get('description', '').lower()
            python_code = task.get('python_code', '').lower()
            
            # Check for strict placeholder indicators
            is_placeholder = False
            for indicator in strict_placeholder_indicators:
                if indicator in task_id or indicator in description or indicator in python_code:
                    is_placeholder = True
                    break
            
            # Check for minimal print-only code (actual placeholders)
            if python_code:
                lines = [line.strip() for line in python_code.split('\n') if line.strip()]
                # If it's just a simple print statement and return, it's likely a placeholder
                if len(lines) <= 2 and all('print(' in line or 'return' in line for line in lines):
                    is_placeholder = True
            
            if not is_placeholder:
                meaningful_count += 1
        
        # At least 80% of tasks should be meaningful
        return meaningful_count >= len(tasks) * 0.8

    def _enhance_with_smart_tasks(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Enhance a DAG spec that has placeholder tasks with meaningful ones."""
        description = spec.get('description', '')
        complexity = spec.get('complexity', 'medium')
        
        # Generate meaningful tasks based on the description
        smart_tasks = self._create_smart_fallback_tasks(description, description.lower())
        
        if smart_tasks:
            # Replace placeholder tasks with smart ones
            spec['tasks'] = smart_tasks
            spec['estimated_tasks'] = len(smart_tasks)
            
            # Update complexity if needed based on task count
            if len(smart_tasks) <= 3:
                spec['complexity'] = 'simple'
            elif len(smart_tasks) <= 8:
                spec['complexity'] = 'medium'
            else:
                spec['complexity'] = 'complex'
            
            logger.info(f"Enhanced DAG spec with {len(smart_tasks)} meaningful tasks")
        
        return spec

    def _fix_common_json_issues(self, response: str) -> Optional[Dict[str, Any]]:
        """Fix common JSON formatting issues that occur in large LLM responses."""
        # Check if response is very large (potential truncation issues)
        if len(response) > 5000:
            print(f"Large response detected ({len(response)} chars), applying special fixes")

        # Fix 1: Handle unterminated string literals at the end
        # Look for cases where strings end abruptly
        lines = response.split('\n')
        fixed_lines = []

        for i, line in enumerate(lines):
            # Fix unterminated strings that end with commas
            if line.rstrip().endswith('",') and not line.count('"') % 2 == 0:
                # Find the last quote and ensure proper termination
                last_quote_idx = line.rfind('"')
                if last_quote_idx > 0:
                    # Check if there's content after the last quote before comma
                    after_quote = line[last_quote_idx + 1:].strip()
                    if after_quote.startswith(','):
                        # This looks like a proper termination, keep as is
                        pass
                    else:
                        # This might be unterminated, add closing quote
                        line = line[:last_quote_idx + 1] + '",' + line[last_quote_idx + 1:]

            # Fix missing colons after property names
            if line.strip().endswith('"') and i < len(lines) - 1:
                next_line = lines[i + 1].strip()
                if next_line.startswith('"') and not line.rstrip().endswith('":'):
                    # Add missing colon
                    line = line.rstrip() + ':'

            # Fix trailing commas in objects/arrays
            line = line.rstrip()
            if line.endswith(',') and i == len(lines) - 1:
                # Last line shouldn't end with comma
                line = line.rstrip(',')

            fixed_lines.append(line)

        response = '\n'.join(fixed_lines)

        # Fix 2: Ensure proper JSON structure
        response = response.strip()
        if not response.startswith('{'):
            response = '{' + response
        if not response.endswith('}'):
            response = response + '}'

        # Fix 3: Handle nested object issues
        # Look for common patterns of malformed nested structures
        response = re.sub(r'}\s*{\s*', '},{', response)  # Fix consecutive objects
        response = re.sub(r']\s*\[\s*', '],[', response)  # Fix consecutive arrays

        try:
            return json.loads(response)
        except json.JSONDecodeError as e:
            print(f"Fixed JSON still invalid: {e}")
            return None


