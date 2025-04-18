import io
import os
import json
import oracledb
from timeit import default_timer as timer
from fdk import response

# Get database connection parameters from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DSN = os.getenv("DSN")

# Check if necessary environment variables exist
if not all([DB_USER, DB_PASSWORD, DSN]):
    missing_keys = [key for key, value in {'DB_USER': DB_USER, 'DB_PASSWORD': DB_PASSWORD, 'DSN': DSN}.items() if not value]
    raise ValueError(f"ERROR: Missing configuration keys: {', '.join(missing_keys)}")

print(f"INFO: DB_USER is {DB_USER}", flush=True)
print(f"INFO: db_password is {DB_PASSWORD}", flush=True)
print(f"INFO: dsn is {DSN}", flush=True)

# Create a database session pool
start_pool = timer()
DB_POOL = oracledb.create_pool(user=DB_USER, password=DB_PASSWORD, dsn=DSN, min=1, max=10)
end_pool = timer()
print(f"INFO: DB pool created in {end_pool - start_pool} sec", flush=True)

def get_user_id_from_path(ctx):
    """Extract the user ID from the request path"""
    path = ctx.RequestURL()
    path_parts = path.strip('/').split('/')
    return path_parts[-1] if len(path_parts) >= 2 and path_parts[-2] == 'users' else None

def execute_db_operation(sql, bind_vars):
    """Execute a database operation"""
    try:
        with DB_POOL.acquire() as db_connection:
            with db_connection.cursor() as db_cursor:
                db_cursor.execute(sql, bind_vars)
                db_connection.commit()
    except Exception as ex:
        print(f'ERROR: Failed to execute database operation: {ex}', flush=True)
        raise

def handler(ctx, data: io.BytesIO = None):
    """Function handler that calls the appropriate processing function based on the request method"""
    try:
        method = ctx.Method()
        method_handlers = {
            'POST': handle_post,
            'GET': handle_get,
            'PUT': handle_put,
            'DELETE': handle_delete
        }
        handler_func = method_handlers.get(method)
        if handler_func:
            return handler_func(ctx, data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
    except Exception as ex:
        print(f'ERROR: Invalid payload: {ex}', flush=True)
        raise

def handle_post(ctx, data: io.BytesIO = None):
    """Handle POST requests to create a user"""
    try:
        payload_bytes = data.getvalue()
        if not payload_bytes:
            raise KeyError('No keys in payload')
        payload = json.loads(payload_bytes)

        user_id = get_user_id_from_path(ctx)
        if not user_id:
            raise ValueError("Missing required fields: user_id")

        required_fields = ["first_name", "last_name", "username"]
        for field in required_fields:
            if field not in payload:
                raise ValueError(f"Missing required field: {field}")

        sql = """
            INSERT INTO users (ID, FIRST_NAME, LAST_NAME, USERNAME)
            VALUES (:1, :2, :3, :4)
        """
        bind_vars = [user_id, payload["first_name"], payload["last_name"], payload["username"]]
        execute_db_operation(sql, bind_vars)
        return response.Response(
            ctx,
            response_data=json.dumps({"message": "User created successfully"}),
            headers={"Content-Type": "application/json"}
        )
    except Exception as ex:
        print(f'ERROR: Invalid payload: {ex}', flush=True)
        raise

def handle_get(ctx, data: io.BytesIO = None):
    """Handle GET requests to retrieve user information"""
    try:
        user_id = get_user_id_from_path(ctx)
        if not user_id:
            return read_all_users(ctx)
        else:
            return read_user(ctx, user_id)
    except Exception as ex:
        print(f'ERROR: Invalid payload: {ex}', flush=True)
        raise

def read_user(ctx, user_id):
    """Read information for a single user"""
    try:
        sql = """
            SELECT ID, FIRST_NAME, LAST_NAME, USERNAME, CREATED_ON
            FROM users
            WHERE ID = :1
        """
        bind_vars = [user_id]
        with DB_POOL.acquire() as db_connection:
            with db_connection.cursor() as db_cursor:
                db_cursor.execute(sql, bind_vars)
                db_cursor.rowfactory = lambda *args: dict(zip([d[0] for d in db_cursor.description], args))
                result = db_cursor.fetchone()
                if result:
                    if result.get("CREATED_ON"):
                        result["CREATED_ON"] = result["CREATED_ON"].isoformat()
                    return response.Response(
                        ctx,
                        response_data=json.dumps(result),
                        headers={"Content-Type": "application/json"}
                    )
                else:
                    return response.Response(
                        ctx,
                        response_data=json.dumps({"message": "User not found"}),
                        headers={"Content-Type": "application/json"}
                    )
    except Exception as ex:
        print(f'ERROR: Failed to read user: {ex}', flush=True)
        raise

def read_all_users(ctx):
    """Read information for all users"""
    try:
        sql = """
            SELECT ID, FIRST_NAME, LAST_NAME, USERNAME, CREATED_ON
            FROM users
        """
        with DB_POOL.acquire() as db_connection:
            with db_connection.cursor() as db_cursor:
                db_cursor.execute(sql)
                db_cursor.rowfactory = lambda *args: dict(zip([d[0] for d in db_cursor.description], args))
                results = db_cursor.fetchall()
                for result in results:
                    if result.get("CREATED_ON"):
                        result["CREATED_ON"] = result["CREATED_ON"].isoformat()
                return response.Response(
                    ctx,
                    response_data=json.dumps(results),
                    headers={"Content-Type": "application/json"}
                )
    except Exception as ex:
        print(f'ERROR: Failed to read all users: {ex}', flush=True)
        raise

def handle_put(ctx, data: io.BytesIO = None):
    """Handle PUT requests to update user information"""
    try:
        payload_bytes = data.getvalue()
        if not payload_bytes:
            raise KeyError('No keys in payload')
        payload = json.loads(payload_bytes)
        return update_user(ctx, payload)
    except Exception as ex:
        print(f'ERROR: Invalid payload: {ex}', flush=True)
        raise

def update_user(ctx, payload):
    """Update user information"""
    try:
        user_id = get_user_id_from_path(ctx)
        if not user_id:
            raise ValueError("Missing required fields: user_id")

        required_fields = ["first_name", "last_name", "username"]
        for field in required_fields:
            if field not in payload:
                raise ValueError(f"Missing required field: {field}")

        sql = """
            UPDATE users
            SET FIRST_NAME = :1, LAST_NAME = :2, USERNAME = :3
            WHERE ID = :4
        """
        bind_vars = [payload["first_name"], payload["last_name"], payload["username"], user_id]
        execute_db_operation(sql, bind_vars)
        return response.Response(
            ctx,
            response_data=json.dumps({"message": "User updated successfully"}),
            headers={"Content-Type": "application/json"}
        )
    except Exception as ex:
        print(f'ERROR: Failed to update user: {ex}', flush=True)
        raise

def handle_delete(ctx, data: io.BytesIO = None):
    """Handle DELETE requests to delete user information"""
    try:
        user_id = get_user_id_from_path(ctx)
        if not user_id:
            raise ValueError("Missing required field: user_id")
        return delete_user(ctx, user_id)
    except Exception as ex:
        print(f'ERROR: Invalid payload: {ex}', flush=True)
        raise

def delete_user(ctx, user_id):
    """Delete user information"""
    try:
        sql = """
            DELETE FROM users
            WHERE ID = :1
        """
        bind_vars = [user_id]
        execute_db_operation(sql, bind_vars)
        return response.Response(
            ctx,
            response_data=json.dumps({"message": "User deleted successfully"}),
            headers={"Content-Type": "application/json"}
        )
    except Exception as ex:
        print(f'ERROR: Failed to delete user: {ex}', flush=True)
        raise
    